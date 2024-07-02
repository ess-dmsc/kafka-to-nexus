// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "ConfigureKafka.h"
#include "Consumer.h"
#include <memory>

namespace Kafka {
std::unique_ptr<Consumer> createConsumer(const BrokerSettings &Settings,
                                         const std::string &Broker);
std::unique_ptr<Consumer> createConsumer(BrokerSettings const &Settings);

class ConsumerFactoryInterface {
public:
  virtual std::shared_ptr<ConsumerInterface>
  createConsumer(BrokerSettings const &Settings) = 0;
  virtual std::shared_ptr<Kafka::ConsumerInterface>
  createConsumerAtOffset(Kafka::BrokerSettings const &settings,
                         std::string const &topic, int partition_id,
                         int64_t offset) = 0;
  virtual ~ConsumerFactoryInterface() = default;
};

class ConsumerFactory : public ConsumerFactoryInterface {
public:
  std::shared_ptr<ConsumerInterface>
  createConsumer(BrokerSettings const &Settings) override;
  std::shared_ptr<Kafka::ConsumerInterface>
  createConsumerAtOffset(Kafka::BrokerSettings const &settings,
                         std::string const &topic, int partition_id,
                         int64_t offset) override;
  ~ConsumerFactory() override = default;
};

class StubConsumerFactory : public Kafka::ConsumerFactoryInterface {
public:
  std::shared_ptr<Kafka::ConsumerInterface> createConsumer(
      [[maybe_unused]] Kafka::BrokerSettings const &settings) override {
    auto consumer = std::make_shared<StubConsumer>(messages);
    return consumer;
  }

  std::shared_ptr<Kafka::ConsumerInterface>
  createConsumerAtOffset([[maybe_unused]] Kafka::BrokerSettings const &settings,
                         std::string const &topic, int partition_id,
                         [[maybe_unused]] int64_t offset) override {
    auto consumer = std::make_shared<StubConsumer>(messages);
    consumer->topic = topic;
    consumer->partition = partition_id;
    return consumer;
  }
  ~StubConsumerFactory() override = default;

  // One set of messages shared by all topics/consumers.
  std::shared_ptr<std::vector<FileWriter::Msg>> messages =
      std::make_shared<std::vector<FileWriter::Msg>>();
};
} // namespace Kafka
