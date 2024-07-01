// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Kafka/ConsumerFactory.h"

#include "CommandListener.h"

#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"
#include "Kafka/PollStatus.h"
#include "Msg.h"
#include <utility>

namespace Command {

CommandListener::CommandListener(uri::URI const &CommandTopicUri,
                                 Kafka::BrokerSettings Settings)
    : KafkaAddress(CommandTopicUri.HostPort),
      CommandTopic(CommandTopicUri.Topic), KafkaSettings(std::move(Settings)) {
  KafkaSettings.Address = CommandTopicUri.HostPort;
}

CommandListener::CommandListener(uri::URI const &CommandTopicUri,
                                 Kafka::BrokerSettings Settings,
                                 time_point StartTimestamp)
    : KafkaAddress(CommandTopicUri.HostPort),
      CommandTopic(CommandTopicUri.Topic), KafkaSettings(std::move(Settings)),
      StartTimestamp(StartTimestamp) {}

std::pair<Kafka::PollStatus, Msg> CommandListener::pollForCommand() {
  if (not KafkaAddress.empty() and not CommandTopic.empty()) {
    if (Consumer == nullptr) {
      setUpConsumer();
    } else {
      return Consumer->poll();
    }
  }
  return {Kafka::PollStatus::TimedOut, Msg()};
}

void CommandListener::setUpConsumer() {
  if (StartTimestamp < time_point::max()) {
    // The CommandListener instance was created to process commands since a
    // specific timestamp
    Consumer = Kafka::createConsumer(KafkaSettings, KafkaAddress);
    Consumer->assignAllPartitions(CommandTopic, StartTimestamp);
  } else {
    Consumer = Kafka::createConsumer(KafkaSettings, KafkaAddress);
    Consumer->addTopic(CommandTopic);
  }
}

void CommandListener::try_connecting_to_topic() {
  try {
    setUpConsumer();
  } catch (std::exception const &error) {
    auto const message =
        fmt::format("Could not connect to command topic {}: {}", CommandTopic,
                    error.what());
    LOG_ERROR(message);
    throw MetadataException(message);
  }
}

} // namespace Command
