// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/ConsumerFactory.h"
#include <gtest/gtest.h>
#include <memory>
#include <trompeloeil.hpp>

using namespace Kafka;
using trompeloeil::_;

class ConsumerFactoryInterfaceMock : public ConsumerFactoryInterface {
public:
  MAKE_MOCK1(createConsumer,
             std::unique_ptr<ConsumerInterface>(BrokerSettings const &),
             override);
  MAKE_MOCK2(getConsumer,
             std::shared_ptr<ConsumerInterface>(std::string_view,
                                                BrokerSettings const &),
             override);
};

class ConsumerFactoryTests : public ::testing::Test {
protected:
  void SetUp() override {
    ConsumerFactoryPtr = std::make_unique<ConsumerFactory>();
  }
  std::unique_ptr<ConsumerFactory> ConsumerFactoryPtr;
};

TEST_F(ConsumerFactoryTests, getConsumerReturnsNewConsumerIfNotFound) {
  BrokerSettings Settings;
  auto Consumer1 = ConsumerFactoryPtr->getConsumer("Id1", Settings);
  auto Consumer2 = ConsumerFactoryPtr->getConsumer("Id2", Settings);
  EXPECT_NE(Consumer1, nullptr);
  EXPECT_NE(Consumer2, nullptr);
  EXPECT_NE(Consumer1, Consumer2);
}

TEST_F(ConsumerFactoryTests, getConsumerReturnsExistingConsumerIfFound) {
  BrokerSettings Settings;
  auto Consumer1 = ConsumerFactoryPtr->getConsumer("Id1", Settings);
  auto Consumer2 = ConsumerFactoryPtr->getConsumer("Id1", Settings);
  EXPECT_EQ(Consumer1, Consumer2);
}

TEST_F(ConsumerFactoryTests, createConsumerAlwaysReturnsNewConsumer) {
  BrokerSettings Settings;
  auto Consumer1 = ConsumerFactoryPtr->createConsumer(Settings);
  auto Consumer2 = ConsumerFactoryPtr->createConsumer(Settings);
  EXPECT_NE(Consumer1, Consumer2);
}
