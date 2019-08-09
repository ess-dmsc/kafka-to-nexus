#include "../KafkaW/Producer.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <trompeloeil.hpp>

using trompeloeil::_;

class ProducerTests : public ::testing::Test {
protected:
  void SetUp() override {}
};

class ProducerStandIn : public KafkaW::Producer {
public:
  explicit ProducerStandIn(KafkaW::BrokerSettings &Settings)
      : Producer(Settings){};
  using Producer::ProducerID;
  using Producer::ProducerPtr;
};

class MockProducer : public RdKafka::Producer {
public:
  MAKE_CONST_MOCK0(name, const std::string(), override);
  MAKE_CONST_MOCK0(memberid, const std::string(), override);
  MAKE_MOCK1(poll, int(int), override);
  MAKE_MOCK0(outq_len, int(), override);
  MAKE_MOCK4(metadata, RdKafka::ErrorCode(bool, const RdKafka::Topic *,
                                          RdKafka::Metadata **, int),
             override);
  MAKE_MOCK1(pause,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK1(resume,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &),
             override);
  MAKE_MOCK5(query_watermark_offsets,
             RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                int64_t *, int),
             override);
  MAKE_MOCK4(get_watermark_offsets,
             RdKafka::ErrorCode(const std::string &, int32_t, int64_t *,
                                int64_t *),
             override);
  MAKE_MOCK2(offsetsForTimes,
             RdKafka::ErrorCode(std::vector<RdKafka::TopicPartition *> &, int),
             override);
  MAKE_MOCK1(get_partition_queue,
             RdKafka::Queue *(const RdKafka::TopicPartition *), override);
  MAKE_MOCK1(set_log_queue, RdKafka::ErrorCode(RdKafka::Queue *), override);
  MAKE_MOCK0(yield, void(), override);
  MAKE_MOCK1(clusterid, const std::string(int), override);
  MAKE_MOCK0(c_ptr, rd_kafka_s *(), override);
  MAKE_MOCK2(create, RdKafka::Producer *(RdKafka::Conf *, std::string));
  MAKE_MOCK7(produce, RdKafka::ErrorCode(RdKafka::Topic *, int32_t, int, void *,
                                         size_t, const std::string *, void *),
             override);
  MAKE_MOCK8(produce, RdKafka::ErrorCode(RdKafka::Topic *, int32_t, int, void *,
                                         size_t, const void *, size_t, void *),
             override);
  MAKE_MOCK9(produce,
             RdKafka::ErrorCode(const std::string, int32_t, int, void *, size_t,
                                const void *, size_t, int64_t, void *),
             override);
  MAKE_MOCK5(produce, RdKafka::ErrorCode(RdKafka::Topic *, int32_t,
                                         const std::vector<char> *,
                                         const std::vector<char> *, void *),
             override);
  MAKE_MOCK1(flush, RdKafka::ErrorCode(int), override);
  MAKE_MOCK1(controllerid, int32_t(int), override);
  MAKE_MOCK1(fatal_error, RdKafka::ErrorCode(std::string &), override);
  MAKE_MOCK5(oauthbearer_set_token,
             RdKafka::ErrorCode(const std::string &, int64_t,
                                const std::string &,
                                const std::list<std::string> &, std::string &),
             override);
  MAKE_MOCK1(oauthbearer_set_token_failure,
             RdKafka::ErrorCode(const std::string &), override);
  MAKE_MOCK10(produce, RdKafka::ErrorCode(std::string, int32_t, int, void *,
                                          size_t, const void *, size_t, int64_t,
                                          RdKafka::Headers *, void *),
              override);
  MAKE_MOCK1(purge, RdKafka::ErrorCode(int), override);
};

// Don't really care if anything gets called with this as it's RdKafka's
// responsibility
class FakeTopic : public RdKafka::Topic {
public:
  FakeTopic() = default;
  ~FakeTopic() override = default;
  const std::string name() const override { return ""; };
  // cppcheck-suppress unusedFunction;
  bool partition_available(int32_t /*partition*/) const override {
    return true;
  };
  // cppcheck-suppress unusedFunction;
  RdKafka::ErrorCode offset_store(int32_t /*partition*/,
                                  int64_t /*offset*/) override {
    return RdKafka::ERR_NO_ERROR;
  };
  struct rd_kafka_topic_s *c_ptr() override {
    return {};
  };
};

TEST_F(ProducerTests, creatingForwarderIncrementsForwarderCounter) {
  KafkaW::BrokerSettings Settings{};
  ProducerStandIn Producer1(Settings);
  ProducerStandIn Producer2(Settings);
  ASSERT_EQ(-1, Producer1.ProducerID - Producer2.ProducerID);
}

TEST_F(ProducerTests, callPollTest) {
  KafkaW::BrokerSettings Settings{};
  auto TempProducerPtr = std::make_unique<MockProducer>();
  REQUIRE_CALL(*TempProducerPtr, poll(_)).TIMES(1).RETURN(0);

  REQUIRE_CALL(*TempProducerPtr, outq_len()).TIMES(2).RETURN(0);
  // Needs to be put in a scope here so we can check that outq_len is called on
  // destruction
  {
    ProducerStandIn Producer(Settings);
    Producer.ProducerPtr = std::move(TempProducerPtr);
    Producer.poll();
  }
}

TEST_F(ProducerTests, produceReturnsNoErrorCodeIfMessageProduced) {
  KafkaW::BrokerSettings Settings{};
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
  KafkaW::BrokerSettings Settings{};
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
  KafkaW::BrokerSettings Settings{};
  auto TempProducerPtr = std::make_unique<MockProducer>();

  REQUIRE_CALL(*TempProducerPtr, outq_len()).TIMES(82).RETURN(1);
  REQUIRE_CALL(*TempProducerPtr, poll(_)).TIMES(80).RETURN(1);

  // Needs to be put in a scope here so we can check that outq_len is called on
  // destruction
  {
    ProducerStandIn Producer(Settings);
    ASSERT_NO_THROW(Producer.ProducerPtr = std::move(TempProducerPtr));
  }
}

TEST_F(ProducerTests, produceAlsoCallsPollOnProducer) {
  KafkaW::BrokerSettings Settings{};
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
