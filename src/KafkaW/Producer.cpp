#include "Producer.h"
#include "logger.h"

namespace KafkaW {

static std::atomic<int> g_kafka_producer_instance_count;

Producer::~Producer() {
  LOG(Sev::Debug, "~Producer");
  if (ProducerPtr) {
    int TimeoutMS = 1;
    int OutQueueLength = 0;
    while (true) {
      OutQueueLength = ProducerPtr->outq_len();
      if (OutQueueLength == 0) {
        break;
      }
      auto EventsHandled = ProducerPtr->poll(TimeoutMS);
      if (EventsHandled > 0) {
        LOG(Sev::Debug,
            "rd_kafka_poll handled: {}  outq before: {}  timeout: {}",
            EventsHandled, OutQueueLength, TimeoutMS);
      }
      TimeoutMS = TimeoutMS << 1;
      if (TimeoutMS > 8192) {
        break;
      }
    }
    if (OutQueueLength > 0) {
      LOG(Sev::Notice,
          "Kafka out queue still not empty: {}, destroying producer anyway.",
          OutQueueLength);
    }
  }
}

Producer::Producer(BrokerSettings Settings)
    : ProducerBrokerSettings(std::move(Settings)) {
  id = g_kafka_producer_instance_count++;

  std::string ErrorString;

  auto Config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  ProducerBrokerSettings.apply(Config);
  Config->set("dr_cb", &DeliveryCb, ErrorString);
  Config->set("event_cb", &EventCb, ErrorString);
  Config->set("metadata.broker.list", ProducerBrokerSettings.Address,
              ErrorString);
  ProducerPtr.reset(RdKafka::Producer::create(Config, ErrorString));
  if (!ProducerPtr) {
    LOG(Sev::Error, "can not create kafka handle: {}", ErrorString);
    throw std::runtime_error("can not create Kafka handle");
  }

  LOG(Sev::Info, "new Kafka producer: {}, with brokers: {}",
      ProducerPtr->name(), ProducerBrokerSettings.Address.c_str());
}

void Producer::poll() {
  auto EventsHandled = ProducerPtr->poll(ProducerBrokerSettings.PollTimeoutMS);
  LOG(Sev::Debug,
      "IID: {}  broker: {}  rd_kafka_poll()  served: {}  outq_len: {}", id,
      ProducerBrokerSettings.Address, EventsHandled, outputQueueLength());
  Stats.poll_served += EventsHandled;
  Stats.out_queue = outputQueueLength();
}

RdKafka::Producer *Producer::getRdKafkaPtr() const { return ProducerPtr.get(); }

int Producer::outputQueueLength() { return ProducerPtr->outq_len(); }
} // namespace KafkaW