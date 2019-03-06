#include "KafkaW/Consumer.h"
#include "KafkaWMocks.h"

#include <Msg.h>
#include <gtest/gtest.h>

using namespace KafkaW;
using trompeloeil::_;

class ConsumerTests : public ::testing::Test {
protected:
  void SetUp() override {
    RdConsumer = std::make_unique<MockKafkaConsumer>();
    //    Consumer = std::make_unique<KafkaW::Consumer>(
    //        RdConsumer, std::make_unique<RdKafka::Conf>(),
    //        std::make_unique<KafkaW::KafkaEventCb>());
  }

  std::unique_ptr<MockKafkaConsumer> RdConsumer;
  //  std::unique_ptr<KafkaW::Consumer> Consumer;

  std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> validMessageToPoll() {
    PollStatus Status;
    FileWriter::Msg KafkaMessage;

    std::pair<PollStatus, FileWriter::Msg> NewPair(Status,
                                                   std::move(KafkaMessage));
    std::unique_ptr<std::pair<PollStatus, FileWriter::Msg>> DataToReturn;
    DataToReturn = std::make_unique<std::pair<PollStatus, FileWriter::Msg>>(
        std::move(NewPair));
    return DataToReturn;
  }
};

TEST_F(ConsumerTests, pollReturnsConsumerMessageWithMessagePollStatus) {
  auto *Message = new MockMessage;
  std::string TestPayload = "Test payload";
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  REQUIRE_CALL(*Message, len()).TIMES(2).RETURN(TestPayload.size());
  RdKafka::MessageTimestamp TimeStamp;
  TimeStamp.timestamp = 1;
  TimeStamp.type = RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME;
  REQUIRE_CALL(*Message, timestamp()).TIMES(2).RETURN(TimeStamp);
  REQUIRE_CALL(*Message, offset()).TIMES(1).RETURN(1);

  REQUIRE_CALL(*Message, payload())
      .TIMES(1)
      .RETURN(
          reinterpret_cast<void *>(const_cast<char *>(TestPayload.c_str())));

  REQUIRE_CALL(*RdConsumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*RdConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage->first, PollStatus::Message);
  }
}

TEST_F(
    ConsumerTests,
    pollReturnsConsumerMessageWithEmptyPollStatusIfKafkaErrorMessageIsEmpty) {
  auto *Message = new MockMessage;
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  REQUIRE_CALL(*Message, len()).TIMES(1).RETURN(0);

  REQUIRE_CALL(*RdConsumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*RdConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage->first, PollStatus::Empty);
  }
}

TEST_F(ConsumerTests,
       pollReturnsConsumerMessageWithEmptyPollStatusIfEndofPartition) {
  auto *Message = new MockMessage;
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR__PARTITION_EOF);

  REQUIRE_CALL(*RdConsumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*RdConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage->first, PollStatus::EndOfPartition);
  }
}

TEST_F(ConsumerTests,
       pollReturnsConsumerMessageWithErrorPollStatusIfUnknownOrUnexpected) {
  auto *Message = new MockMessage;
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR__BAD_MSG);

  REQUIRE_CALL(*RdConsumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*RdConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage->first, PollStatus::Error);
  }
}

TEST_F(ConsumerTests, getTopicPartitionNumbersThrowsErrorIfTopicsEmpty) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);
  auto TopicVector = RdKafka::Metadata::TopicMetadataVector{};

  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);

  REQUIRE_CALL(*MockConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);

  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopic("something"), std::runtime_error);
  }
}

TEST_F(ConsumerTests, getTopicPartitionNumbersThrowsErrorIfTopicDoesntExist) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);

  auto TopicMetadata = std::unique_ptr<MockTopicMetadata>(
      new MockTopicMetadata("not_something"));
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  REQUIRE_CALL(*Metadata, topics()).TIMES((1)).RETURN(&TopicVector);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopic("something"), std::runtime_error);
  }
}

TEST_F(ConsumerTests,
       getTopicPartitionNumbersReturnsPartitionNumbersIfTopicDoesExist) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);

  auto TopicMetadata =
      std::unique_ptr<MockTopicMetadata>(new MockTopicMetadata("something"));
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  auto PartitionMetadata =
      std::unique_ptr<MockPartitionMetadata>(new MockPartitionMetadata);
  auto PartitionMetadataVector =
      RdKafka::TopicMetadata::PartitionMetadataVector{PartitionMetadata.get()};

  REQUIRE_CALL(*MockConsumer, query_watermark_offsets(_, _, _, _, _))
      .TIMES((1))
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  REQUIRE_CALL(*Metadata, topics()).TIMES((1)).RETURN(&TopicVector);
  REQUIRE_CALL(*TopicMetadata, partitions())
      .TIMES((1))
      .RETURN(&PartitionMetadataVector);
  REQUIRE_CALL(*PartitionMetadata, id()).TIMES((1)).RETURN(1);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  REQUIRE_CALL(*MockConsumer, assign(_))
      .TIMES((1))
      .RETURN(RdKafka::ERR_NO_ERROR);
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    ASSERT_NO_THROW(Consumer->addTopic("something"));
  }
}

TEST_F(ConsumerTests,
       testTopicPresentReturnsFalseIfTopicDoesntExistInMetadata) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);
  auto TopicMetadata =
      std::unique_ptr<MockTopicMetadata>(new MockTopicMetadata("something"));
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};

  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);

  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    ASSERT_TRUE(Consumer->topicPresent("something"));
  }
}

TEST_F(ConsumerTests, testTopicPresentReturnsTrueIfTopicDoesExistInMetadata) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);
  auto TopicMetadata =
      std::unique_ptr<MockTopicMetadata>(new MockTopicMetadata("something"));
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};

  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);

  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    ASSERT_FALSE(Consumer->topicPresent("somethingelse"));
  }
}

TEST_F(ConsumerTests, testAddTopicErrorCantgetOffestsForTimestamp) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  auto TopicMetadata =
      std::unique_ptr<MockTopicMetadata>(new MockTopicMetadata("something"));

  auto PartitionMetadata =
      std::unique_ptr<MockPartitionMetadata>(new MockPartitionMetadata);
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  auto PartitionMetadataVector =
      RdKafka::TopicMetadata::PartitionMetadataVector{PartitionMetadata.get()};
  REQUIRE_CALL(*PartitionMetadata, id()).TIMES((1)).RETURN(1);
  REQUIRE_CALL(*TopicMetadata, partitions())
      .TIMES((1))
      .RETURN(&PartitionMetadataVector);
  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);

  REQUIRE_CALL(*MockConsumer, offsetsForTimes(_, _))
      .TIMES((1))
      .RETURN(RdKafka::ERR__UNKNOWN_TOPIC);
  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopicAtTimestamp("something",
                                               std::chrono::milliseconds{1}),
                 std::runtime_error);
  }
}

TEST_F(ConsumerTests, testAddTopicAtTimestampErrorWhileSubscribingToOffsets) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  auto TopicMetadata =
      std::unique_ptr<MockTopicMetadata>(new MockTopicMetadata("something"));

  auto PartitionMetadata =
      std::unique_ptr<MockPartitionMetadata>(new MockPartitionMetadata);
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  auto PartitionMetadataVector =
      RdKafka::TopicMetadata::PartitionMetadataVector{PartitionMetadata.get()};
  REQUIRE_CALL(*PartitionMetadata, id()).TIMES((1)).RETURN(1);
  REQUIRE_CALL(*TopicMetadata, partitions())
      .TIMES((1))
      .RETURN(&PartitionMetadataVector);
  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);
  REQUIRE_CALL(*MockConsumer, offsetsForTimes(_, _))
      .TIMES((1))
      .RETURN(RdKafka::ERR_NO_ERROR);
  REQUIRE_CALL(*MockConsumer, assign(_))
      .TIMES((1))
      .RETURN(RdKafka::ERR__EXISTING_SUBSCRIPTION);

  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopicAtTimestamp("something",
                                               std::chrono::milliseconds{1}),
                 std::runtime_error);
  }
}

TEST_F(ConsumerTests, testAddTopicAtTimestampSuccess) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  auto TopicMetadata =
      std::unique_ptr<MockTopicMetadata>(new MockTopicMetadata("something"));

  auto PartitionMetadata =
      std::unique_ptr<MockPartitionMetadata>(new MockPartitionMetadata);
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  auto PartitionMetadataVector =
      RdKafka::TopicMetadata::PartitionMetadataVector{PartitionMetadata.get()};
  REQUIRE_CALL(*PartitionMetadata, id()).TIMES((1)).RETURN(1);
  REQUIRE_CALL(*TopicMetadata, partitions())
      .TIMES((1))
      .RETURN(&PartitionMetadataVector);
  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);
  REQUIRE_CALL(*MockConsumer, offsetsForTimes(_, _))
      .TIMES((1))
      .RETURN(RdKafka::ERR_NO_ERROR);
  REQUIRE_CALL(*MockConsumer, assign(_))
      .TIMES((1))
      .RETURN(RdKafka::ERR_NO_ERROR);

  {
    auto Consumer = std::make_unique<KafkaW::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<KafkaW::KafkaEventCb>());
    EXPECT_NO_THROW(Consumer->addTopicAtTimestamp(
        "something", std::chrono::milliseconds{1}));
  }
}