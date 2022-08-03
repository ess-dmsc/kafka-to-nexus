// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "../Kafka/Producer.h"
#include "helpers/RdKafkaMocks.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <trompeloeil.hpp>

using trompeloeil::_;

class ProducerTests : public ::testing::Test {
protected:
  void SetUp() override {}
};

class ProducerStandIn : public Kafka::Producer {
public:
  explicit ProducerStandIn(Kafka::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

// Don't really care if anything gets called with this as it's RdKafka's
// responsibility
class FakeTopic : public RdKafka::Topic {
public:
  FakeTopic() = default;
  ~FakeTopic() override = default;
  const std::string name() const override { return ""; };
  bool partition_available(int32_t /*partition*/) const override {
    return true;
  };
  RdKafka::ErrorCode offset_store(int32_t /*partition*/,
                                  int64_t /*offset*/) override {
    return RdKafka::ERR_NO_ERROR;
  };
  struct rd_kafka_topic_s *c_ptr() override {
    return {};
  };
};

TEST_F(ProducerTests, produceReturnsNoErrorCodeIfMessageProduced) {
  Kafka::BrokerSettings Settings{};
  auto TempProducerPtr = std::make_unique<MockProducer>();
  REQUIRE_CALL(*TempProducerPtr, produce(_, _, _, _, _, _, _, _))
      .TIMES(1)
      .RETURN(RdKafka::ERR_NO_ERROR);

  REQUIRE_CALL(*TempProducerPtr, outq_len()).TIMES(1).RETURN(0);
  ALLOW_CALL(*TempProducerPtr, poll(_)).RETURN(1);

  // Needs to be put in a scope here so we can check that outq_len is called on
  // destruction
  {
    ProducerStandIn Producer(Settings);
    Producer.ProducerPtr = std::move(TempProducerPtr);
    ASSERT_EQ(
        Producer.produce(new FakeTopic, 0, 0, nullptr, 0, nullptr, 0, nullptr),
        RdKafka::ErrorCode::ERR_NO_ERROR);
  }
}

TEST_F(ProducerTests, produceReturnsErrorCodeIfMessageNotProduced) {
  Kafka::BrokerSettings Settings{};
  auto TempProducerPtr = std::make_unique<MockProducer>();
  REQUIRE_CALL(*TempProducerPtr, produce(_, _, _, _, _, _, _, _))
      .TIMES(1)
      .RETURN(RdKafka::ERR__BAD_MSG);

  REQUIRE_CALL(*TempProducerPtr, outq_len()).TIMES(1).RETURN(0);
  ALLOW_CALL(*TempProducerPtr, poll(_)).RETURN(1);

  // Needs to be put in a scope here so we can check that outq_len is called on
  // destruction
  {
    ProducerStandIn Producer(Settings);
    Producer.ProducerPtr = std::move(TempProducerPtr);
    ASSERT_EQ(
        Producer.produce(new FakeTopic, 0, 0, nullptr, 0, nullptr, 0, nullptr),
        RdKafka::ErrorCode::ERR__BAD_MSG);
  }
}

TEST_F(ProducerTests, testDestructorOutputQueueWithTooManyItemsToProduce) {
  Kafka::BrokerSettings Settings{};
  auto TempProducerPtr = std::make_unique<MockProducer>();

  REQUIRE_CALL(*TempProducerPtr, outq_len()).TIMES(82).RETURN(1);
  REQUIRE_CALL(*TempProducerPtr, poll(_)).TIMES(80).RETURN(1);

  // Needs to be put in a scope here so we can check that outq_len is called on
  // destruction
  {
    ASSERT_NO_THROW(ProducerStandIn(Settings).ProducerPtr =
                        std::move(TempProducerPtr));
  }
}

TEST_F(ProducerTests, produceAlsoCallsPollOnProducer) {
  Kafka::BrokerSettings Settings{};
  auto TempProducerPtr = std::make_unique<MockProducer>();

  // We'll call produce this many times and require the same number of calls to
  // poll
  const uint32_t NumberOfProduceCalls = 3;

  REQUIRE_CALL(*TempProducerPtr, produce(_, _, _, _, _, _, _, _))
      .TIMES(NumberOfProduceCalls)
      .RETURN(RdKafka::ERR_NO_ERROR);

  ALLOW_CALL(*TempProducerPtr, outq_len()).RETURN(0);

  // This is what we are testing; that poll gets called once for each time that
  // produce is called.
  // This is really important as if we don't call poll we do not handle
  // successful publish events and messages never get cleared from librdkafka's
  // producer queue, eventually the queue fills up and we stop being able to
  // publish messages
  REQUIRE_CALL(*TempProducerPtr, poll(_)).TIMES(NumberOfProduceCalls).RETURN(1);

  ProducerStandIn TestProducer(Settings);
  TestProducer.ProducerPtr = std::move(TempProducerPtr);

  for (uint32_t CallNumber = 0; CallNumber < NumberOfProduceCalls;
       ++CallNumber) {
    TestProducer.produce(new FakeTopic, 0, 0, nullptr, 0, nullptr, 0, nullptr);
  }
}
