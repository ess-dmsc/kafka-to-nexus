#include "Producer.h"
#include "logger.h"
#include <vector>

namespace KafkaW {

static std::atomic<int> g_kafka_producer_instance_count;

void Producer::cb_delivered(rd_kafka_t *RK, rd_kafka_message_t const *Message,
                            void *Opaque) {
  auto Self = reinterpret_cast<Producer *>(Opaque);
  if (!Message) {
    LOG(Sev::Error, "IID: {} msg should never be null", Self->id);
    ++Self->Stats.produce_cb_fail;
    return;
  }
  if (Message->err) {
    LOG(Sev::Error, "IID: {} failure on delivery, {}, topic {}, {} [{}] {}",
        Self->id, rd_kafka_name(RK), rd_kafka_topic_name(Message->rkt),
        rd_kafka_err2name(Message->err), Message->err,
        rd_kafka_err2str(Message->err));
    if (Message->err == RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) {
    }
    if (auto &CallBack = Self->on_delivery_failed) {
      CallBack(Message);
    }
    ++Self->Stats.produce_cb_fail;
  } else {
    if (auto &CallBack = Self->on_delivery_ok) {
      CallBack(Message);
    }
    ++Self->Stats.produce_cb;
  }
}

void Producer::cb_error(rd_kafka_t *RK, int ErrorCode, char const *ErrorMessage,
                        void *Opaque) {
  auto Self = reinterpret_cast<Producer *>(Opaque);
  auto Error = static_cast<rd_kafka_resp_err_t>(ErrorCode);
  Sev Level = Sev::Warning;
  if (Error == RD_KAFKA_RESP_ERR__TRANSPORT) {
    Level = Sev::Error;
  } else {
    if (Self->on_error)
      Self->on_error(Self, Error);
  }
  LOG(Level, "Kafka cb_error id: {}  broker: {}  errno: {}  errorname: {}  "
             "errorstring: {}  message: {}",
      Self->id, Self->ProducerBrokerSettings.Address, ErrorCode,
      rd_kafka_err2name(Error), rd_kafka_err2str(Error), ErrorMessage);
}

int Producer::cb_stats(rd_kafka_t *RK, char *JSON, size_t JSONLength,
                       void *Opaque) {
  auto Self = reinterpret_cast<Producer *>(Opaque);
  LOG(Sev::Debug, "IID: {}  INFO cb_stats {} length {}   {:.{}}", Self->id,
      rd_kafka_name(RK), JSONLength, JSON, JSONLength);
  // What does librdkafka want us to return from this callback?
  return 0;
}

void Producer::cb_log(rd_kafka_t const *RK, int Level, char const *Fac,
                      char const *Buf) {
  auto Self = reinterpret_cast<Producer *>(rd_kafka_opaque(RK));
  LOG(Sev::Debug, "IID: {}  {}  Fac: {}", Self->id, Buf, Fac);
}

void Producer::cb_throttle(rd_kafka_t *RK, char const *BrokerName,
                           int32_t BrokerID, int ThrottleTime_ms,
                           void *Opaque) {
  auto Time = reinterpret_cast<Producer *>(Opaque);
  LOG(Sev::Debug, "IID: {}  INFO cb_throttle  BrokerID: {}  broker_name: {}  "
                  "throttle_time_ms: {}",
      Time->id, BrokerID, BrokerName, ThrottleTime_ms);
}

Producer::~Producer() {
  LOG(Sev::Debug, "~Producer");
  if (RdKafkaPtr) {
    int Timeout_ms = 1;
    uint32_t OutputQueueLength = 0;
    while (true) {
      OutputQueueLength = rd_kafka_outq_len(RdKafkaPtr);
      if (OutputQueueLength == 0) {
        break;
      }
      auto EventsHandled = rd_kafka_poll(RdKafkaPtr, Timeout_ms);
      if (EventsHandled > 0) {
        LOG(Sev::Debug,
            "rd_kafka_poll handled: {}  outq before: {}  timeout: {}",
            EventsHandled, OutputQueueLength, Timeout_ms);
      }
      Timeout_ms = Timeout_ms << 1;
      if (Timeout_ms > 8 * 1024) {
        break;
      }
    }
    if (OutputQueueLength > 0) {
      LOG(Sev::Notice,
          "Kafka out queue still not empty: {}  destroy producer anyway.",
          OutputQueueLength);
    }
    LOG(Sev::Debug, "rd_kafka_destroy");
    rd_kafka_destroy(RdKafkaPtr);
    RdKafkaPtr = nullptr;
  }
}

Producer::Producer(BrokerSettings const &Settings)
    : ProducerBrokerSettings(Settings) {
  id = g_kafka_producer_instance_count++;

  // librdkafka API sometimes wants to write errors into a buffer:
  std::vector<char> Errors;
  Errors.resize(512);

  rd_kafka_conf_t *Configuration = 0;
  Configuration = rd_kafka_conf_new();
  rd_kafka_conf_set_dr_msg_cb(Configuration, Producer::cb_delivered);
  rd_kafka_conf_set_error_cb(Configuration, Producer::cb_error);
  rd_kafka_conf_set_stats_cb(Configuration, Producer::cb_stats);
  rd_kafka_conf_set_log_cb(Configuration, Producer::cb_log);
  rd_kafka_conf_set_throttle_cb(Configuration, Producer::cb_throttle);

  rd_kafka_conf_set_opaque(Configuration, this);
  LOG(Sev::Debug, "Producer opaque: {}", (void *)this);

  ProducerBrokerSettings.apply(Configuration);

  RdKafkaPtr = rd_kafka_new(RD_KAFKA_PRODUCER, Configuration, Errors.data(),
                            Errors.size());
  if (!RdKafkaPtr) {
    LOG(Sev::Error, "can not create kafka handle: {}", Errors.data());
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
  int EventsHandled =
      rd_kafka_poll(RdKafkaPtr, ProducerBrokerSettings.PollTimeoutMS);
  LOG(Sev::Debug,
      "IID: {}  broker: {}  rd_kafka_poll()  served: {}  outq_len: {}", id,
      ProducerBrokerSettings.Address, EventsHandled, outputQueueLength());
  Stats.poll_served += EventsHandled;
  Stats.out_queue = outputQueueLength();
}

rd_kafka_t *Producer::getRdKafkaPtr() const { return RdKafkaPtr; }

uint64_t Producer::outputQueueLength() { return rd_kafka_outq_len(RdKafkaPtr); }
}
