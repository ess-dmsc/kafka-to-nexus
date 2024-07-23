// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/JobListener.h"
#include "Kafka/MetadataException.h"
#include <gtest/gtest.h>

using std::chrono_literals::operator""ms;

TEST(JobListener, first_poll_timesout_because_consumer_not_connected) {
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto Listener =
      Command::JobListener::create_null("pool_topic", {}, consumer_factory);

  auto const [poll_status, message] = Listener->pollForJob();
  EXPECT_EQ(poll_status, Kafka::PollStatus::TimedOut);
}

TEST(JobListener, when_no_messages_poll_hits_EOP) {
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto Listener =
      Command::JobListener::create_null("pool_topic", {}, consumer_factory);
  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForJob();

  auto const [poll_status, message] = Listener->pollForJob();
  EXPECT_EQ(poll_status, Kafka::PollStatus::EndOfPartition);
}

TEST(JobListener, finds_message_when_present) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = 123456ms;
  metadata.Offset = 0;
  metadata.Partition = 1;
  metadata.topic = "pool_topic";

  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  consumer_factory->messages->emplace_back("::some_flatbuffer::", 21, metadata);

  auto Listener =
      Command::JobListener::create_null("pool_topic", {}, consumer_factory);
  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForJob();

  auto const [poll_status, message] = Listener->pollForJob();
  EXPECT_EQ(poll_status, Kafka::PollStatus::Message);
  EXPECT_EQ(message.size(), 21u);
  EXPECT_EQ(message.getMetaData().topic, "pool_topic");
}

TEST(JobListener, after_disconnecting_from_pool_polling_reconnects) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = 123456ms;
  metadata.Offset = 0;
  metadata.Partition = 1;
  metadata.topic = "pool_topic";

  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  consumer_factory->messages->emplace_back("::some_flatbuffer::", 21, metadata);

  auto Listener =
      Command::JobListener::create_null("pool_topic", {}, consumer_factory);

  Listener->disconnectFromPool();

  // First poll always returns TimedOut because it has to connect the consumer.
  auto const [first_poll_status, first_message] = Listener->pollForJob();
  EXPECT_EQ(first_poll_status, Kafka::PollStatus::TimedOut);
  // Second poll returns the message
  auto const [poll_status, message] = Listener->pollForJob();
  EXPECT_EQ(poll_status, Kafka::PollStatus::Message);
  EXPECT_EQ(message.size(), 21u);
  EXPECT_EQ(message.getMetaData().topic, "pool_topic");
}
