#include "../KafkaW/Producer.h"
#include "MockMessage.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <trompeloeil.hpp>

using trompeloeil::_;
using namespace KafkaW;

class ProducerDeliveryCbTests : public ::testing::Test {
protected:
  void SetUp() override {}
};

struct ProducerMessageStandIn : ProducerMessage {
  ProducerMessageStandIn(std::function<void()> DestructorFunction)
      : Fun(DestructorFunction) {}
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
