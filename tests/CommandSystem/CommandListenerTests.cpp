// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/CommandListener.h"
#include <gtest/gtest.h>

using std::chrono_literals::operator""ms;

TEST(CommandListener, when_no_messages_poll_times_out) {
  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  auto Listener = Command::CommandListener::create_null(
      uri::URI("localhost:1111/some_topic"), {}, consumer_factory);

  auto const [poll_status, message] = Listener->pollForCommand();
  EXPECT_EQ(poll_status, Kafka::PollStatus::TimedOut);
  EXPECT_EQ(message.size(), 0u);
}

TEST(CommandListener, finds_message_when_present) {
  FileWriter::MessageMetaData metadata;
  metadata.Timestamp = 123456ms;
  metadata.Offset = 0;
  metadata.Partition = 1;
  metadata.topic = "some_topic";

  auto consumer_factory = std::make_shared<Kafka::StubConsumerFactory>();
  consumer_factory->messages->emplace_back("::some_flatbuffer::", 21, metadata);

  auto Listener = Command::CommandListener::create_null(
      uri::URI("localhost:1111/some_topic"), {}, consumer_factory);

  // First poll always returns TimedOut because it has to connect the consumer.
  Listener->pollForCommand();

  auto const [poll_status, message] = Listener->pollForCommand();
  EXPECT_EQ(poll_status, Kafka::PollStatus::Message);
  EXPECT_EQ(message.size(), 21u);
}
