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

class StubConsumerFactory : public ConsumerFactoryInterface {
public:
  std::shared_ptr<ConsumerInterface>
  createConsumer([[maybe_unused]] BrokerSettings const &Settings) override {
    return std::make_shared<StubConsumer>();
  };
  std::shared_ptr<Kafka::ConsumerInterface>
  createConsumerAtOffset(Kafka::BrokerSettings const &settings,
                         [[maybe_unused]] std::string const &topic,
                         [[maybe_unused]] int partition_id,
                         [[maybe_unused]] int64_t offset) override {
    return createConsumer(settings);
  }
  ~StubConsumerFactory() override = default;
};
} // namespace Kafka
