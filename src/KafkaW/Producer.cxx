#include "Producer.h"
#include "logger.h"

namespace KafkaW {

static std::atomic<int> g_kafka_producer_instance_count;

void ProducerMsg::deliveryOk() {}

void ProducerMsg::deliveryError() {}

void Producer::cb_delivered(rd_kafka_t *rk, rd_kafka_message_t const *msg,
                            void *opaque) {
  auto self = reinterpret_cast<Producer *>(opaque);
  if (!msg) {
    LOG(Sev::Error, "IID: {}  ERROR msg should never be null", self->id);
    ++self->Stats.produce_cb_fail;
    return;
  }
  if (msg->err) {
    LOG(Sev::Error, "IID: {}  ERROR on delivery, {}, topic {}, {} [{}] {}",
        self->id, rd_kafka_name(rk), rd_kafka_topic_name(msg->rkt),
        rd_kafka_err2name(msg->err), msg->err, rd_kafka_err2str(msg->err));
    if (msg->err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) {
      // TODO
    }
    if (auto &cb = self->on_delivery_failed) {
      cb(msg);
    }
    ++self->Stats.produce_cb_fail;
  } else {
    if (auto &cb = self->on_delivery_ok) {
      cb(msg);
    }
    if (false) {
      LOG(Sev::Debug, "IID: {}  Ok delivered ({}, p {}, offset {}, len {})",
          self->id, rd_kafka_name(rk), msg->partition, msg->offset, msg->len);
    }
    ++self->Stats.produce_cb;
  }
}

void Producer::cb_error(rd_kafka_t *rk, int err_i, char const *msg,
                        void *opaque) {
  auto self = reinterpret_cast<Producer *>(opaque);
  auto err = static_cast<rd_kafka_resp_err_t>(err_i);
  Sev ll = Sev::Warning;
  if (err == RD_KAFKA_RESP_ERR__TRANSPORT) {
    ll = Sev::Error;
    // rd_kafka_dump(stdout, rk);
  } else {
    if (self->on_error)
      self->on_error(self, err);
  }
  LOG(ll, "Kafka cb_error id: {}  broker: {}  errno: {}  errorname: {}  "
          "errorstring: {}  message: {}",
      self->id, self->ProducerBrokerSettings.Address, err_i,
      rd_kafka_err2name(err), rd_kafka_err2str(err), msg);
}

int Producer::cb_stats(rd_kafka_t *rk, char *json, size_t json_len,
                       void *opaque) {
  auto self = reinterpret_cast<Producer *>(opaque);
  LOG(Sev::Debug, "IID: {}  INFO cb_stats {} length {}   {:.{}}", self->id,
      rd_kafka_name(rk), json_len, json, json_len);
  // What does librdkafka want us to return from this callback?
  return 0;
}

void Producer::cb_log(rd_kafka_t const *rk, int level, char const *fac,
                      char const *buf) {
  auto self = reinterpret_cast<Producer *>(rd_kafka_opaque(rk));
  LOG(Sev::Debug, "IID: {}  {}  fac: {}", self->id, buf, fac);
}

void Producer::cb_throttle(rd_kafka_t *rk, char const *broker_name,
                           int32_t broker_id, int throttle_time_ms,
                           void *opaque) {
  auto self = reinterpret_cast<Producer *>(opaque);
  LOG(Sev::Debug, "IID: {}  INFO cb_throttle  broker_id: {}  broker_name: {}  "
                  "throttle_time_ms: {}",
      self->id, broker_id, broker_name, throttle_time_ms);
}

Producer::~Producer() {
  LOG(Sev::Debug, "~Producer");
  if (RdKafkaPtr) {
    int timeout_ms = 1;
    uint32_t outq_len = 0;
    while (true) {
      outq_len = rd_kafka_outq_len(RdKafkaPtr);
      if (outq_len == 0) {
        break;
      }
      auto events_handled = rd_kafka_poll(RdKafkaPtr, timeout_ms);
      if (events_handled > 0) {
        LOG(Sev::Debug,
            "rd_kafka_poll handled: {}  outq before: {}  timeout: {}",
            events_handled, outq_len, timeout_ms);
      }
      timeout_ms = timeout_ms << 1;
      if (timeout_ms > 8 * 1024) {
        break;
      }
    }
    if (outq_len > 0) {
      LOG(Sev::Notice,
          "Kafka out queue still not empty: {}  destroy producer anyway.",
          outq_len);
    }
    LOG(Sev::Debug, "rd_kafka_destroy");
    rd_kafka_destroy(RdKafkaPtr);
    RdKafkaPtr = nullptr;
  }
}

Producer::Producer(BrokerSettings ProducerBrokerSettings)
    : ProducerBrokerSettings(ProducerBrokerSettings) {
  id = g_kafka_producer_instance_count++;

  // librdkafka API sometimes wants to write errors into a buffer:
  std::vector<char> errstr;
  errstr.resize(512);

  rd_kafka_conf_t *conf = 0;
  conf = rd_kafka_conf_new();
  rd_kafka_conf_set_dr_msg_cb(conf, Producer::cb_delivered);
  rd_kafka_conf_set_error_cb(conf, Producer::cb_error);
  rd_kafka_conf_set_stats_cb(conf, Producer::cb_stats);
  rd_kafka_conf_set_log_cb(conf, Producer::cb_log);
  rd_kafka_conf_set_throttle_cb(conf, Producer::cb_throttle);

  rd_kafka_conf_set_opaque(conf, this);
  LOG(Sev::Debug, "Producer opaque: {}", (void *)this);

  ProducerBrokerSettings.apply(conf);

  RdKafkaPtr =
      rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr.data(), errstr.size());
  if (!RdKafkaPtr) {
    LOG(Sev::Error, "can not create kafka handle: {}", errstr.data());
    throw std::runtime_error("can not create Kafka handle");
  }

  rd_kafka_set_log_level(RdKafkaPtr, 4);

  LOG(Sev::Info, "New Kafka {} with brokers: {}", rd_kafka_name(RdKafkaPtr),
      ProducerBrokerSettings.Address.c_str());
  if (rd_kafka_brokers_add(RdKafkaPtr,
                           ProducerBrokerSettings.Address.c_str()) == 0) {
    LOG(Sev::Error, "could not add brokers");
    throw std::runtime_error("could not add brokers");
  }
}

Producer::Producer(Producer &&x) {
  using std::swap;
  swap(RdKafkaPtr, x.RdKafkaPtr);
  swap(on_delivery_ok, x.on_delivery_ok);
  swap(on_delivery_failed, x.on_delivery_failed);
  swap(on_error, x.on_error);
  swap(ProducerBrokerSettings, x.ProducerBrokerSettings);
  swap(id, x.id);
}

void Producer::poll() {
  int events_handled =
      rd_kafka_poll(RdKafkaPtr, ProducerBrokerSettings.PollTimeoutMS);
  LOG(Sev::Debug,
      "IID: {}  broker: {}  rd_kafka_poll()  served: {}  outq_len: {}", id,
      ProducerBrokerSettings.Address, events_handled, outputQueueLength());
  if (log_level >= 8) {
    rd_kafka_dump(stdout, RdKafkaPtr);
  }
  Stats.poll_served += events_handled;
  Stats.out_queue = outputQueueLength();
}

void Producer::pollWhileOutputQueueFilled() {
  while (outputQueueLength() > 0) {
    Stats.poll_served +=
        rd_kafka_poll(RdKafkaPtr, ProducerBrokerSettings.PollTimeoutMS);
  }
}

rd_kafka_t *Producer::getRdKafkaPtr() const { return RdKafkaPtr; }

uint64_t Producer::outputQueueLength() { return rd_kafka_outq_len(RdKafkaPtr); }

uint64_t Producer::totalMessagesProduced() { return TotalMessagesProduced; }

ProducerStats::ProducerStats(ProducerStats const &x) {
  produced = x.produced.load();
  produce_fail = x.produce_fail.load();
  local_queue_full = x.local_queue_full.load();
  produce_cb = x.produce_cb.load();
  produce_cb_fail = x.produce_cb_fail.load();
  poll_served = x.poll_served.load();
  msg_too_large = x.msg_too_large.load();
  produced_bytes = x.produced_bytes.load();
  out_queue = x.out_queue.load();
}
}
