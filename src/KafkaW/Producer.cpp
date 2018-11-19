#include "Producer.h"
#include "logger.h"
#include <vector>

namespace KafkaW {

static std::atomic<int> g_kafka_producer_instance_count;

void Producer::cb_delivered(rd_kafka_t *RK, rd_kafka_message_t const *Message,
                            void *Opaque) {
  auto self = reinterpret_cast<Producer *>(Opaque);
  if (!Message) {
    LOG(Sev::Error, "IID: {} msg should never be null", self->id);
    ++self->Stats.produce_cb_fail;
    return;
  }
  if (Message->err) {
    LOG(Sev::Error, "IID: {} failure on delivery, {}, topic {}, {} [{}] {}",
        self->id, rd_kafka_name(RK), rd_kafka_topic_name(Message->rkt),
        rd_kafka_err2name(Message->err), Message->err,
        rd_kafka_err2str(Message->err));
    if (Message->err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) {
      // TODO
    }
    if (auto &cb = self->on_delivery_failed) {
      cb(Message);
    }
    ++self->Stats.produce_cb_fail;
  } else {
    if (auto &cb = self->on_delivery_ok) {
      cb(Message);
    }
    if (false) {
      LOG(Sev::Debug, "IID: {}  Ok delivered ({}, p {}, offset {}, len {})",
          self->id, rd_kafka_name(RK), Message->partition, Message->offset,
          Message->len);
    }
    ++self->Stats.produce_cb;
  }
}

void Producer::cb_error(rd_kafka_t *RK, int ERR_code, char const *ERR_message,
                        void *Opaque) {
  auto self = reinterpret_cast<Producer *>(Opaque);
  auto err = static_cast<rd_kafka_resp_err_t>(ERR_code);
  Sev ll = Sev::Warning;
  if (err == RD_KAFKA_RESP_ERR__TRANSPORT) {
    ll = Sev::Error;
  } else {
    if (self->on_error)
      self->on_error(self, err);
  }
  LOG(ll, "Kafka cb_error id: {}  broker: {}  errno: {}  errorname: {}  "
          "errorstring: {}  message: {}",
      self->id, self->ProducerBrokerSettings.Address, ERR_code,
      rd_kafka_err2name(err), rd_kafka_err2str(err), ERR_message);
}

int Producer::cb_stats(rd_kafka_t *RK, char *Json, size_t Json_length,
                       void *Opaque) {
  auto self = reinterpret_cast<Producer *>(Opaque);
  LOG(Sev::Debug, "IID: {}  INFO cb_stats {} length {}   {:.{}}", self->id,
      rd_kafka_name(RK), Json_length, Json, Json_length);
  // What does librdkafka want us to return from this callback?
  return 0;
}

void Producer::cb_log(rd_kafka_t const *RK, int Level, char const *Fac,
                      char const *Buf) {
  auto self = reinterpret_cast<Producer *>(rd_kafka_opaque(RK));
  LOG(Sev::Debug, "IID: {}  {}  Fac: {}", self->id, Buf, Fac);
}

void Producer::cb_throttle(rd_kafka_t *RK, char const *BrokerName,
                           int32_t BrokerID, int Throttle_time_ms,
                           void *Opaque) {
  auto self = reinterpret_cast<Producer *>(Opaque);
  LOG(Sev::Debug, "IID: {}  INFO cb_throttle  BrokerID: {}  broker_name: {}  "
                  "throttle_time_ms: {}",
      self->id, BrokerID, BrokerName, Throttle_time_ms);
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

rd_kafka_t *Producer::getRdKafkaPtr() const { return RdKafkaPtr; }

uint64_t Producer::outputQueueLength() { return rd_kafka_outq_len(RdKafkaPtr); }

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
