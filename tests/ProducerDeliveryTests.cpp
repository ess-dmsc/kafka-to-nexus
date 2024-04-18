// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/Producer.h"
#include "helpers/MockMessage.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <trompeloeil.hpp>

using trompeloeil::_;
using namespace Kafka;

class ProducerDeliveryCbTests : public ::testing::Test {
protected:
  void SetUp() override {}
};

struct ProducerMessageStandIn : ProducerMessage {
  explicit ProducerMessageStandIn(std::function<void()> DestructorFunction)
      : Fun(std::move(DestructorFunction)) {}
  ~ProducerMessageStandIn() override { Fun(); }
  std::function<void()> Fun;
};

TEST_F(ProducerDeliveryCbTests,
       deliveryCbIncrementsProduceStatsOnSuccessAndReleasesOpaquePointer) {
  bool Called = false;

  ProducerMessageStandIn *FakeMessage =
      new ProducerMessageStandIn([&Called]() { Called = true; });
  MockMessage Message;
  auto *TempPtr = reinterpret_cast<RdKafka::Message *>(&Message);
  REQUIRE_CALL(Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  REQUIRE_CALL(Message, msg_opaque())
      .TIMES(1)
      .RETURN(reinterpret_cast<void *>(FakeMessage));
  ProducerStats Stats;
  ProducerDeliveryCb Callback(Stats);
  Callback.dr_cb(*TempPtr);
  ASSERT_TRUE(Called);
  EXPECT_EQ(Stats.produce_cb, uint64_t(1));
  EXPECT_EQ(Stats.produce_cb_fail, uint64_t(0));
}

TEST_F(ProducerDeliveryCbTests,
       deliveryCbIncrementsProduceFailStatsOnFailureAndReleasesOpaquePointer) {
  bool Called = false;

  ProducerMessageStandIn *FakeMessage =
      new ProducerMessageStandIn([&Called]() { Called = true; });
  MockMessage Message;
  auto *TempPtr = reinterpret_cast<RdKafka::Message *>(&Message);
  REQUIRE_CALL(Message, err())
      .TIMES(2)
      .RETURN(RdKafka::ErrorCode::ERR__BAD_MSG);
  REQUIRE_CALL(Message, topic_name()).TIMES(1).RETURN("");
  REQUIRE_CALL(Message, errstr()).TIMES(1).RETURN("");
  REQUIRE_CALL(Message, msg_opaque())
      .TIMES(1)
      .RETURN(reinterpret_cast<void *>(FakeMessage));
  ProducerStats Stats;
  ProducerDeliveryCb Callback(Stats);
  Callback.dr_cb(*TempPtr);
  ASSERT_TRUE(Called);
  EXPECT_EQ(Stats.produce_cb, uint64_t(0));
  EXPECT_EQ(Stats.produce_cb_fail, uint64_t(1));
}
