#include "Producer.h"
#include "logger.h"

namespace KafkaW {

static std::atomic<int> ProducerInstanceCount;

Producer::~Producer() {
  Logger->debug("~Producer");
  if (ProducerPtr != nullptr) {
    int TimeoutMS = 100;
    int NumberOfIterations = 80;
    for (int i = 0; i < NumberOfIterations; i++) {
      if (outputQueueLength() == 0) {
        return;
      }
      ProducerPtr->poll(TimeoutMS);
    }
    if (outputQueueLength() > 0) {
      Logger->trace(
          "Kafka out queue still not empty: {}, destroying producer anyway.",
          outputQueueLength());
    }
  }
}

void Producer::setConf(std::string &ErrorString) {
  try {
    ProducerBrokerSettings.apply(Conf.get());
  } catch (std::runtime_error &e) {
    throw std::runtime_error(
        "Cannot create kafka handle due to configuration error");
  }

  Conf->set("dr_cb", &DeliveryCb, ErrorString);
  Conf->set("event_cb", &EventCb, ErrorString);
  Conf->set("metadata.broker.list", ProducerBrokerSettings.Address,
            ErrorString);
}

Producer::Producer(BrokerSettings &Settings)
    : ProducerBrokerSettings(Settings) {
  ProducerID = ProducerInstanceCount++;

  std::string ErrorString;
  setConf(ErrorString);
  ProducerPtr = std::unique_ptr<RdKafka::Producer>(
      RdKafka::Producer::create(Conf.get(), ErrorString));
  if (ProducerPtr == nullptr) {
    Logger->error("can not create kafka handle: {}", ErrorString);
    throw std::runtime_error("can not create Kafka handle");
  }

  Logger->info("new Kafka producer: {}, with brokers: {}", ProducerPtr->name(),
               ProducerBrokerSettings.Address.c_str());
}

void Producer::poll() {
  auto EventsHandled = ProducerPtr->poll(ProducerBrokerSettings.PollTimeoutMS);
  auto OutputQueueLength = outputQueueLength();
  Logger->debug(
      "IID: {}  broker: {}  rd_kafka_poll()  served: {}  outq_len: {}",
      ProducerID, ProducerBrokerSettings.Address, EventsHandled,
      OutputQueueLength);
  Stats.poll_served += EventsHandled;
  Stats.out_queue = OutputQueueLength;
}

RdKafka::Producer *Producer::getRdKafkaPtr() const { return ProducerPtr.get(); }

int Producer::outputQueueLength() { return ProducerPtr->outq_len(); }

RdKafka::ErrorCode Producer::produce(RdKafka::Topic *Topic, int32_t Partition,
                                     int MessageFlags, void *Payload,
                                     size_t PayloadSize, const void *Key,
                                     size_t KeySize, void *OpaqueMessage) {
  return ProducerPtr->produce(Topic, Partition, MessageFlags, Payload,
                              PayloadSize, Key, KeySize, OpaqueMessage);
}
} // namespace KafkaW
