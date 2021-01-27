// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "../Kafka/MetadataException.h"
#include "Kafka/Consumer.h"
#include "helpers/MockMessage.h"
#include "helpers/RdKafkaMocks.h"

#include <gtest/gtest.h>
#include <memory>

using namespace Kafka;
using trompeloeil::_;

class ConsumerTests : public ::testing::Test {
protected:
  void SetUp() override { RdConsumer = std::make_unique<MockKafkaConsumer>(); }

  std::unique_ptr<MockKafkaConsumer> RdConsumer;
};

TEST_F(ConsumerTests, pollReturnsConsumerMessageWithMessagePollStatus) {
  auto *Message = new MockMessage;
  std::string const TestPayload = "Test payload";
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  // cppcheck-suppress knownArgument
  REQUIRE_CALL(*Message, len()).TIMES(1).RETURN(TestPayload.size());
  RdKafka::MessageTimestamp TimeStamp;
  TimeStamp.timestamp = 1;
  TimeStamp.type = RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME;
  REQUIRE_CALL(*Message, timestamp()).TIMES(2).RETURN(TimeStamp);
  REQUIRE_CALL(*Message, offset()).TIMES(1).RETURN(1);
  ALLOW_CALL(*Message, partition()).RETURN(0);

  REQUIRE_CALL(*Message, payload())
      .TIMES(1)
      .RETURN(
          reinterpret_cast<void *>(const_cast<char *>(TestPayload.c_str())));

  REQUIRE_CALL(*RdConsumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*RdConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage.first, PollStatus::Message);
  }
}

TEST_F(ConsumerTests, pollReturnsConsumerMessageWithEmptyPollStatusIfEndofPartition) {
  auto *Message = new MockMessage;
  REQUIRE_CALL(*Message, err())
      .TIMES(1)
      .RETURN(RdKafka::ErrorCode::ERR__PARTITION_EOF);

  REQUIRE_CALL(*RdConsumer, consume(_)).TIMES(1).RETURN(Message);
  REQUIRE_CALL(*RdConsumer, close()).TIMES(1).RETURN(RdKafka::ERR_NO_ERROR);
  // Put this in scope to call standin destructor
  {
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage.first, PollStatus::EndOfPartition);
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
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(RdConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    auto ConsumedMessage = Consumer->poll();
    ASSERT_EQ(ConsumedMessage.first, PollStatus::Error);
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
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopic("something"), std::runtime_error);
  }
}

TEST_F(ConsumerTests, getTopicPartitionNumbersThrowsErrorIfTopicDoesntExist) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);

  auto TopicMetadata = std::make_unique<MockTopicMetadata>("not_something");
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  REQUIRE_CALL(*Metadata, topics()).TIMES((1)).RETURN(&TopicVector);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  {
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopic("something"), std::runtime_error);
  }
}

TEST_F(ConsumerTests,
       getTopicPartitionNumbersReturnsPartitionNumbersIfTopicDoesExist) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR_NO_ERROR, Metadata);

  auto TopicMetadata = std::make_unique<MockTopicMetadata>("something");
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  auto PartitionMetadata = std::make_unique<MockPartitionMetadata>();
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
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    ASSERT_NO_THROW(Consumer->addTopic("something"));
  }
}

TEST_F(ConsumerTests, testUpdatingMetadataMultipleTimes) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR__TRANSPORT, Metadata);
  REQUIRE_CALL(*MockConsumer, close())
      .TIMES((1))
      .RETURN(RdKafka::ERR__TRANSPORT);
  auto TopicMetadata = std::make_unique<MockTopicMetadata>("something");
  auto TopicVector =
      RdKafka::Metadata::TopicMetadataVector{TopicMetadata.get()};
  auto PartitionMetadata = std::make_unique<MockPartitionMetadata>();
  auto PartitionMetadataVector =
      RdKafka::TopicMetadata::PartitionMetadataVector{PartitionMetadata.get()};
  REQUIRE_CALL(*Metadata, topics()).TIMES(1).RETURN(&TopicVector);
  REQUIRE_CALL(*TopicMetadata, partitions())
      .TIMES((1))
      .RETURN(&PartitionMetadataVector);
  REQUIRE_CALL(*PartitionMetadata, id()).TIMES((1)).RETURN(1);
  REQUIRE_CALL(*MockConsumer, query_watermark_offsets(_, _, _, _, _))
      .TIMES((1))
      .RETURN(RdKafka::ErrorCode::ERR_NO_ERROR);
  REQUIRE_CALL(*MockConsumer, assign(_))
      .TIMES((1))
      .RETURN(RdKafka::ERR_NO_ERROR);
  {
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    EXPECT_NO_THROW(Consumer->addTopic("something"));
  }
}

TEST_F(ConsumerTests, testMetadataCallThrowsAnError) {
  auto Metadata = new MockMetadata;
  auto MockConsumer = std::make_unique<MockKafkaConsumer>(
      RdKafka::ErrorCode::ERR__ALL_BROKERS_DOWN, Metadata);
  REQUIRE_CALL(*MockConsumer, close()).TIMES((1)).RETURN(RdKafka::ERR_NO_ERROR);
  {
    auto Consumer = std::make_unique<Kafka::Consumer>(
        std::move(MockConsumer),
        std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        std::make_unique<Kafka::KafkaEventCb>());
    EXPECT_THROW(Consumer->addTopic("something"), MetadataException);
  }
}
