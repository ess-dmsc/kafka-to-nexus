// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "KafkaW/MetaDataQueryImpl.h"
#include "helpers/KafkaWMocks.h"
#include <gtest/gtest.h>

 class CreateKafkaHandleTest : public ::testing::Test {
};

 TEST_F(CreateKafkaHandleTest, Success) {
  auto Result = KafkaW::getKafkaHandle<RdKafka::Consumer,
  RdKafka::Conf>("some_broker"); EXPECT_FALSE(Result == nullptr);
}

 class UsedProducerMock : public MockProducer {
 public:
  static int CallsToCreate;
  static RdKafka::Producer* create(RdKafka::Conf *, std::string&) {
    UsedProducerMock::CallsToCreate++;
    return nullptr;
  }
};
 int UsedProducerMock::CallsToCreate = 0;

 TEST_F(CreateKafkaHandleTest, FailedToCreateHandle) {
  UsedProducerMock::CallsToCreate = 0;
  auto testFunc = [](auto Adr) { //Work around for GTest limitation
    return KafkaW::getKafkaHandle<UsedProducerMock,RdKafka::Conf>(Adr);
  };
  EXPECT_THROW(testFunc("some_broker"), MetadataException);
  EXPECT_EQ(UsedProducerMock::CallsToCreate, 1);
}

 class UsedConfMock : public MockConf {
 public:
  static int CallsToCreate;
  static int CallsToSet;
  static UsedConfMock* create(RdKafka::Conf::ConfType) {
    UsedConfMock::CallsToCreate++;
    return new UsedConfMock;
  }
  Conf::ConfResult set(std::string const&, std::string const&, std::string&)
  override {
    UsedConfMock::CallsToSet++;
    return RdKafka::Conf::CONF_INVALID;
  }
};
 int UsedConfMock::CallsToCreate = 0;
 int UsedConfMock::CallsToSet = 0;

 TEST_F(CreateKafkaHandleTest, FailedToSetBroker) {
  UsedProducerMock::CallsToCreate = 0;
  UsedConfMock::CallsToCreate = 0;
  UsedConfMock::CallsToSet = 0;
  auto testFunc = [](auto Adr) { //Work around for GTest limitation
    return KafkaW::getKafkaHandle<UsedProducerMock,UsedConfMock>(Adr);
  };
  EXPECT_THROW(testFunc("some_broker"), MetadataException);
  EXPECT_EQ(UsedConfMock::CallsToCreate, 1);
  EXPECT_EQ(UsedProducerMock::CallsToCreate, 0);
}

 class GetTopicOffsetTest : public ::testing::Test {
};

 using std::chrono_literals::operator""ms;

 class UsedMockPartitionMetadata : public MockPartitionMetadata {
 public:
   UsedMockPartitionMetadata(int Id) : UsedId(Id){}
   int32_t id() const override {
     return UsedId;
   }
   int UsedId;
 };

 class UsedMockTopicMetadata : public MockTopicMetadata {
 public:
   UsedMockTopicMetadata(std::string Name, std::vector<int> Partitions) : MockTopicMetadata(std::move(Name)) {
     for (auto Id : Partitions) {
       ReturnVector.push_back(new UsedMockPartitionMetadata(Id));
     }
   }
   PartitionMetadataVector const * partitions() const override {
     return &ReturnVector;
   }
   PartitionMetadataVector ReturnVector;
 };

 class UsedMockMetadata : public MockMetadata {
 public:
   UsedMockMetadata() {
     ReturnVector.push_back(new UsedMockTopicMetadata("some_topic", {1,3,5}));
     ReturnVector.push_back(new UsedMockTopicMetadata("some_other_topic", {}));
   }
   ~UsedMockMetadata() {
     for (auto &Item : ReturnVector) {
       delete Item;
     }
   }
   TopicMetadataVector const* topics() const override {
     return &ReturnVector;
   }
   TopicMetadataVector ReturnVector;
 };

 static const int RETURN_TIME_OFFSET{1111};
 class UsedProducerMockAlt : public MockProducer {
 public:
  static RdKafka::ErrorCode ReturnErrorCode;
  static int TimeOut;
  static RdKafka::Producer* create(RdKafka::Conf *, std::string&) {
    return new UsedProducerMockAlt;
  }
  RdKafka::ErrorCode offsetsForTimes(std::vector<RdKafka::TopicPartition*>
  &Offsets, int UsedTimeOut) override {
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

 TEST_F(GetTopicOffsetTest, Success) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  std::vector<int> Partitions{4,5};
  std::vector<std::pair<int, int64_t>> ExpectedResult{{4,RETURN_TIME_OFFSET}, {5,RETURN_TIME_OFFSET}};
  EXPECT_EQ(KafkaW::getOffsetForTimeImpl<UsedProducerMockAlt>("Some_broker",
  "some_topic", Partitions, std::chrono::system_clock::now(), 10ms),
            ExpectedResult);
}

 TEST_F(GetTopicOffsetTest, Failure) {
  auto UsedTimeOut{234ms};
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR__BAD_MSG;
  EXPECT_THROW(KafkaW::getOffsetForTimeImpl<UsedProducerMockAlt>("Some_broker",
  "some_topic", {4}, std::chrono::system_clock::now(), UsedTimeOut),
  MetadataException);
  EXPECT_EQ(std::chrono::duration_cast<std::chrono::milliseconds>(UsedTimeOut).count(),
  UsedProducerMockAlt::TimeOut);
}

 class GetTopicPartitionsTest : public ::testing::Test {
};

 class TopicMockAlt : MockTopic {
 public:
   static RdKafka::Topic* create(RdKafka::Handle*, const std::string&,
                                 RdKafka::Conf*, std::string&) {
     return new TopicMockAlt;
   }
 };

 TEST_F(GetTopicPartitionsTest, Success) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  auto ExpectedPartitions = std::vector<int>{1,3,5};
  auto ReceivedPartitions = KafkaW::getPartitionsForTopicImpl<UsedProducerMockAlt,TopicMockAlt>("Some_broker",
                                                                                             "some_topic", 10ms);
  EXPECT_EQ(ReceivedPartitions, ExpectedPartitions);
}

TEST_F(GetTopicPartitionsTest, MetadataFailure) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR__TIMED_OUT;
  try {
    KafkaW::getPartitionsForTopicImpl<UsedProducerMockAlt,TopicMockAlt>("Some_broker",
                                                                        "some_topic", 10ms);
    FAIL() << "Did not throw";
  } catch (MetadataException &E) {
    SUCCEED();
  } catch (std::exception &E) {
    FAIL() << "Got the wrong exception.";
  }
}

class GetTopicNamesTest : public ::testing::Test {
};

TEST_F(GetTopicNamesTest, Success) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR_NO_ERROR;
  auto ExpectedTopics = std::set<std::string>{"some_topic", "some_other_topic"};
  auto ReceivedTopics = KafkaW::getTopicListImpl<UsedProducerMockAlt>("Some_broker", 10ms);
  EXPECT_EQ(ReceivedTopics, ExpectedTopics);
}

TEST_F(GetTopicNamesTest, MetadataFailure) {
  UsedProducerMockAlt::ReturnErrorCode = RdKafka::ErrorCode::ERR__TIMED_OUT;
  EXPECT_THROW(KafkaW::getTopicListImpl<UsedProducerMockAlt>("Some_broker", 10ms), MetadataException);
}