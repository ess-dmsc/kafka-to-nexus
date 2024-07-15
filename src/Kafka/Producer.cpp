// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Producer.h"

#include "ConfigureKafka.h"
#include "logger.h"
#include <utility>

namespace Kafka {

Producer::~Producer() {
  Logger::Debug("~Producer");
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
      Logger::Trace(
          "Kafka out queue still not empty: {}, destroying producer anyway.",
          outputQueueLength());
    }
  }
}

void Producer::setConf(std::string &ErrorString) {
  try {
    configureKafka(*Conf, ProducerBrokerSettings);
  } catch (std::runtime_error &e) {
    throw std::runtime_error(
        "Cannot create kafka handle due to configuration error");
  }

  Conf->set("dr_cb", &DeliveryCb, ErrorString);
  Conf->set("event_cb", &EventCb, ErrorString);
  Conf->set("metadata.broker.list", ProducerBrokerSettings.Address,
            ErrorString);
}

Producer::Producer(BrokerSettings const &Settings)
    : ProducerBrokerSettings(Settings) {
  static std::atomic<int> ProducerInstanceCount;
  ProducerID = ProducerInstanceCount++;

  std::string ErrorString;
  setConf(ErrorString);
  ProducerPtr = std::unique_ptr<RdKafka::Producer>(
      RdKafka::Producer::create(Conf.get(), ErrorString));
  if (ProducerPtr == nullptr) {
    Logger::Critical("Can not create kafka handle: {}", ErrorString);
    throw std::runtime_error("can not create Kafka handle");
  }

  Logger::Info("new Kafka producer: {}, with brokers: {}", ProducerPtr->name(),
               ProducerBrokerSettings.Address);
}

void Producer::poll() {
  auto EventsHandled =
      ProducerPtr->poll(toMilliSeconds(ProducerBrokerSettings.PollTimeout));
  auto OutputQueueLength = outputQueueLength();
  Logger::Debug(
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
  // Do a non-blocking poll of the local producer (note this is not polling
  // anything across the network)
  // NB, if we don't call poll then we haven't handled successful publishing of
  // each message and the messages therefore never get removed from librdkafka's
  // producer queue
  ProducerPtr->poll(0);

  return ProducerPtr->produce(Topic, Partition, MessageFlags, Payload,
                              PayloadSize, Key, KeySize, OpaqueMessage);
}
} // namespace Kafka
