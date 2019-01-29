#include "../KafkaW/Producer.h"
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <trompeloeil.hpp>

// using namespace KafkaW;
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
};

// Don't really care if anything gets called with this as it's RdKafka's
// responsibility
class FakeTopic : public RdKafka::Topic {
public:
  FakeTopic() = default;
  ~FakeTopic() override = default;
  const std::string name() const override { return ""; };
  bool partition_available(int32_t partition) const override { return true; };
  RdKafka::ErrorCode offset_store(int32_t partition, int64_t offset) override {
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
  ProducerStandIn Producer1(Settings);
  Producer1.ProducerPtr = std::make_unique<MockProducer>();
  REQUIRE_CALL(*dynamic_cast<MockProducer *>(Producer1.ProducerPtr.get()),
               poll(_))
      .TIMES(1)
      .RETURN(1);

  ALLOW_CALL(*dynamic_cast<MockProducer *>(Producer1.ProducerPtr.get()),
             outq_len())
      .RETURN(0);

  Producer1.poll();
  ASSERT_EQ(Producer1.Stats.poll_served, uint64_t(1));
}

TEST_F(ProducerTests, produceReturnsNoErrorCodeIfMessageProduced) {
  KafkaW::BrokerSettings Settings{};
  ProducerStandIn Producer1(Settings);
  Producer1.ProducerPtr = std::make_unique<MockProducer>();
  REQUIRE_CALL(*dynamic_cast<MockProducer *>(Producer1.ProducerPtr.get()),
               produce(_, _, _, _, _, _, _, _))
      .TIMES(1)
      .RETURN(RdKafka::ERR_NO_ERROR);
  ASSERT_EQ(
      Producer1.produce(new FakeTopic, 0, 0, nullptr, 0, nullptr, 0, nullptr),
      RdKafka::ErrorCode::ERR_NO_ERROR);
}

TEST_F(ProducerTests, produceReturnsErrorCodeIfMessageNotProduced) {
  KafkaW::BrokerSettings Settings{};
  ProducerStandIn Producer1(Settings);
  Producer1.ProducerPtr = std::make_unique<MockProducer>();
  REQUIRE_CALL(*dynamic_cast<MockProducer *>(Producer1.ProducerPtr.get()),
               produce(_, _, _, _, _, _, _, _))
      .TIMES(1)
      .RETURN(RdKafka::ERR__BAD_MSG);
  ASSERT_EQ(
      Producer1.produce(new FakeTopic, 0, 0, nullptr, 0, nullptr, 0, nullptr),
      RdKafka::ErrorCode::ERR__BAD_MSG);
}
