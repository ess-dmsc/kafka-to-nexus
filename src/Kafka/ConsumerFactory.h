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
  virtual std::unique_ptr<ConsumerInterface>
  createConsumer(BrokerSettings const &Settings) = 0;
  virtual std::shared_ptr<ConsumerInterface>
  getConsumer(std::string_view Id, BrokerSettings const &Settings) = 0;
  virtual ~ConsumerFactoryInterface() = default;
};

class ConsumerFactory : public ConsumerFactoryInterface {
public:
  std::unique_ptr<ConsumerInterface>
  createConsumer(BrokerSettings const &Settings) override;
  std::shared_ptr<ConsumerInterface>
  getConsumer(std::string_view Id, BrokerSettings const &Settings) override;
  ~ConsumerFactory() override = default;

private:
  std::unordered_map<std::string, std::shared_ptr<ConsumerInterface>>
      ConsumerMap;
  std::mutex ConsumerMapMutex;
};

class StubConsumerFactory : public ConsumerFactoryInterface {
public:
  std::unique_ptr<ConsumerInterface>
  createConsumer(BrokerSettings const &Settings) override {
    UNUSED_ARG(Settings);
    return std::unique_ptr<ConsumerInterface>(new StubConsumer());
  };
  std::shared_ptr<ConsumerInterface>
  getConsumer(std::string_view Id, BrokerSettings const &Settings) override {
    UNUSED_ARG(Id);
    UNUSED_ARG(Settings);
    return std::unique_ptr<ConsumerInterface>(new StubConsumer());
  };
  ~StubConsumerFactory() override = default;
};
} // namespace Kafka
