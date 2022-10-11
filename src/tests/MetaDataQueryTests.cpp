// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/BrokerSettings.h"
#include "Kafka/MetaDataQueryImpl.h"
#include "helpers/RdKafkaMocks.h"
#include <gtest/gtest.h>

class UsedProducerMock {
public:
  static int CallsToCreate;
  static RdKafka::Producer *create(RdKafka::Conf *, std::string &) {
    UsedProducerMock::CallsToCreate++;
    return nullptr;
  }
};
int UsedProducerMock::CallsToCreate = 0;

class UsedConfMock : public MockConf {
public:
  static int CallsToCreate;
  static int CallsToSet;
  static UsedConfMock *create(RdKafka::Conf::ConfType) {
    UsedConfMock::CallsToCreate++;
    return new UsedConfMock;
  }
  Conf::ConfResult set(std::string const &, std::string const &,
                       std::string &) override {
    UsedConfMock::CallsToSet++;
    return RdKafka::Conf::CONF_INVALID;
  }
};
int UsedConfMock::CallsToCreate = 0;
int UsedConfMock::CallsToSet = 0;

class CreateKafkaHandleTest : public ::testing::Test {
  void SetUp() override {
    UsedProducerMock::CallsToCreate = 0;
    UsedConfMock::CallsToCreate = 0;
    UsedConfMock::CallsToSet = 0;
  }
};

TEST_F(CreateKafkaHandleTest, OnSuccess) {
  Kafka::BrokerSettings BrokerSettings = {};
  auto Result =
      Kafka::getKafkaHandle<RdKafka::Consumer, RdKafka::Conf>("some_broker", BrokerSettings);
  EXPECT_FALSE(Result == nullptr);
}

TEST_F(CreateKafkaHandleTest, FailureToCreateHandleThrows) {
  auto testFunc = [](auto Adr, auto BrokerSettings) { // Work around for GTest limitation
    return Kafka::getKafkaHandle<UsedProducerMock, RdKafka::Conf>(Adr, BrokerSettings);
  };
  Kafka::BrokerSettings BrokerSettings = {};
  EXPECT_THROW(testFunc("some_broker", BrokerSettings), MetadataException);
  EXPECT_EQ(UsedProducerMock::CallsToCreate, 1);
}

TEST_F(CreateKafkaHandleTest, FailureToSetBrokerThrows) {
  auto testFunc = [](auto Adr, auto BrokerSettings) { // Work around for GTest limitation
    return Kafka::getKafkaHandle<UsedProducerMock, UsedConfMock>(Adr, BrokerSettings);
  };
  Kafka::BrokerSettings BrokerSettings = {};
  EXPECT_THROW(testFunc("some_broker", BrokerSettings), MetadataException);
  EXPECT_EQ(UsedConfMock::CallsToCreate, 1);
  EXPECT_EQ(UsedProducerMock::CallsToCreate, 0);
}

class GetTopicOffsetTest : public ::testing::Test {};

using std::chrono_literals::operator""ms;

class UsedMockPartitionMetadata : public MockPartitionMetadata {
public:
  explicit UsedMockPartitionMetadata(int Id) : UsedId(Id) {}
  int32_t id() const override { return UsedId; }
  int UsedId;
};

class UsedMockTopicMetadata : public MockTopicMetadata {
public:
  UsedMockTopicMetadata(std::string Name, std::vector<int> Partitions)
      : MockTopicMetadata(std::move(Name)) {
    std::transform(Partitions.begin(), Partitions.end(),
                   std::back_inserter(ReturnVector),
                   [](auto Id) { return new UsedMockPartitionMetadata(Id); });
  }
  PartitionMetadataVector const *partitions() const override {
    return &ReturnVector;
  }
  PartitionMetadataVector ReturnVector;
};

class UsedMockMetadata : public MockMetadata {
public:
  UsedMockMetadata() {
    ReturnVector.push_back(new UsedMockTopicMetadata("some_topic", {1, 3, 5}));
    ReturnVector.push_back(new UsedMockTopicMetadata("some_other_topic", {}));
  }
  ~UsedMockMetadata() {
    for (auto &Item : ReturnVector) {
      delete Item;
    }
  }
  TopicMetadataVector const *topics() const override { return &ReturnVector; }
  TopicMetadataVector ReturnVector;
};

static const int RETURN_TIME_OFFSET{1111};
class UsedProducerMockAlt : public MockProducer {
public:
  static RdKafka::ErrorCode ReturnErrorCode;
  static int TimeOut;
  static RdKafka::Producer *create(RdKafka::Conf *, std::string &) {
    return new UsedProducerMockAlt;
  }
  RdKafka::ErrorCode
  offsetsForTimes(std::vector<RdKafka::TopicPartition *> &Offsets,
                  int UsedTimeOut) override {
    for (auto Offset : Offsets) {
      Offset->set_offset(RETURN_TIME_OFFSET);
    }
    UsedProducerMockAlt::TimeOut = UsedTimeOut;
    return UsedProducerMockAlt::ReturnErrorCode;
  }
  RdKafka::ErrorCode metadata(bool, const RdKafka::Topic *,
                              RdKafka::Metadata **MetaDataPtr, int) override {
    *MetaDataPtr = new UsedMockMetadata;
    return UsedProducerMockAlt::ReturnErrorCode;
  }
};
int UsedProducerMockAlt::TimeOut;
RdKafka::ErrorCode UsedProducerMockAlt::ReturnErrorCode =
    RdKafka::ErrorCode::ERR_NO_ERROR;

TEST_F(GetTopicOffsetTest, OnSuccess) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  std::vector<int> Partitions{4, 5};
  Kafka::BrokerSettings BrokerSettings;
  std::vector<std::pair<int, int64_t>> ExpectedResult{{4, RETURN_TIME_OFFSET},
                                                      {5, RETURN_TIME_OFFSET}};
  EXPECT_EQ(Kafka::getOffsetForTimeImpl<UsedProducerMockAlt>(
                "Some_broker", "some_topic", Partitions,
                std::chrono::system_clock::now(), 10ms, BrokerSettings),
            ExpectedResult);
}

TEST_F(GetTopicOffsetTest, OnFailureThrowsAndGetsSetToTimeOut) {
  auto UsedTimeOut{234ms};
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR__BAD_MSG;
  Kafka::BrokerSettings BrokerSettings;
  EXPECT_THROW(Kafka::getOffsetForTimeImpl<UsedProducerMockAlt>(
                   "Some_broker", "some_topic", {4},
                   std::chrono::system_clock::now(), UsedTimeOut,
                   BrokerSettings),
               MetadataException);
  EXPECT_EQ(std::chrono::duration_cast<std::chrono::milliseconds>(UsedTimeOut)
                .count(),
            UsedProducerMockAlt::TimeOut);
}

class GetTopicPartitionsTest : public ::testing::Test {};

class TopicMockAlt {
public:
  static RdKafka::Topic *create(RdKafka::Handle *, const std::string &,
                                RdKafka::Conf *, std::string &) {
    return nullptr;
  }
};

TEST_F(GetTopicPartitionsTest, OnSuccess) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  auto ExpectedPartitions = std::vector<int>{1, 3, 5};
  Kafka::BrokerSettings BrokerSettings;
  auto ReceivedPartitions =
      Kafka::getPartitionsForTopicImpl<UsedProducerMockAlt, TopicMockAlt>(
          "Some_broker", "some_topic", 10ms, BrokerSettings);
  EXPECT_EQ(ReceivedPartitions, ExpectedPartitions);
}

TEST_F(GetTopicPartitionsTest, OnGetPartitionsFailureThrows) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR__TIMED_OUT;
  Kafka::BrokerSettings BrokerSettings;
  EXPECT_THROW(
      (Kafka::getPartitionsForTopicImpl<UsedProducerMockAlt, TopicMockAlt>(
          "Some_broker", "some_topic", 10ms, BrokerSettings)),
      MetadataException);
}

class GetTopicNamesTest : public ::testing::Test {};

TEST_F(GetTopicNamesTest, OnSuccess) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  auto ExpectedTopics = std::set<std::string>{"some_topic", "some_other_topic"};
  Kafka::BrokerSettings BrokerSettings;
  auto ReceivedTopics =
      Kafka::getTopicListImpl<UsedProducerMockAlt>("Some_broker", 10ms,
        BrokerSettings);
  EXPECT_EQ(ReceivedTopics, ExpectedTopics);
}

TEST_F(GetTopicNamesTest, MetadataFailure) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR__TIMED_OUT;
  Kafka::BrokerSettings BrokerSettings;
  EXPECT_THROW(
      Kafka::getTopicListImpl<UsedProducerMockAlt>("Some_broker", 10ms,
        BrokerSettings),
      MetadataException);
}
