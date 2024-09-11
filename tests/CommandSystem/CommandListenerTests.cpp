// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/CommandListener.h"
#include "Kafka/MetadataException.h"
#include <gtest/gtest.h>

using std::chrono_literals::operator""ms;

TEST(CommandListener, first_poll_timesout_because_consumer_not_connected) {
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto Listener =
      Command::CommandListener::create_null("some_topic", {}, consumer_factory);

  auto const [poll_status, message] = Listener->pollForCommand();
  EXPECT_EQ(poll_status, Kafka::PollStatus::TimedOut);
}

TEST(CommandListener, when_no_messages_poll_hits_EOP) {
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto Listener =
      Command::CommandListener::create_null("some_topic", {}, consumer_factory);
  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForCommand();

  auto const [poll_status, message] = Listener->pollForCommand();
  EXPECT_EQ(poll_status, Kafka::PollStatus::EndOfPartition);
}

TEST(CommandListener, finds_message_when_present) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = 123456ms;
  metadata.Offset = 0;
  metadata.Partition = 1;
  metadata.topic = "some_topic";

  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  consumer_factory->messages->emplace_back("::some_flatbuffer::", 20, metadata);

  auto Listener =
      Command::CommandListener::create_null("some_topic", {}, consumer_factory);
  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForCommand();

  auto const [poll_status, message] = Listener->pollForCommand();
  EXPECT_EQ(poll_status, Kafka::PollStatus::Message);
  EXPECT_EQ(message.size(), 20u);
  EXPECT_EQ(message.getMetaData().topic, "some_topic");
}

TEST(CommandListener, change_topic) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = 123456ms;
  metadata.Offset = 0;
  metadata.Partition = 1;
  metadata.topic = "new_topic";

  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  consumer_factory->messages->emplace_back("::some_flatbuffer::", 20, metadata);

  auto Listener =
      Command::CommandListener::create_null("some_topic", {}, consumer_factory);
  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForCommand();

  Listener->change_topic("new_topic");

  auto const [poll_status, message] = Listener->pollForCommand();
  EXPECT_EQ(poll_status, Kafka::PollStatus::Message);
  EXPECT_EQ(message.size(), 20u);
  EXPECT_EQ(message.getMetaData().topic, "new_topic");
}

TEST(CommandListener, change_topic_throws_if_topic_does_not_exist) {
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  consumer_factory->valid_topics.emplace_back("valid_topic");

  auto Listener = Command::CommandListener::create_null("valid_topic", {},
                                                        consumer_factory);
  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForCommand();

  EXPECT_THROW(Listener->change_topic("invalid_topic"), MetadataException);
}
