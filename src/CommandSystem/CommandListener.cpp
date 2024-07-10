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

CommandListener::CommandListener(
    std::string const &command_topic, Kafka::BrokerSettings const &settings,
    time_point start_timestamp,
    std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory)
    : CommandTopic(command_topic), KafkaSettings(settings),
      StartTimestamp(start_timestamp),
      _consumer_factory(std::move(consumer_factory)) {}

std::pair<Kafka::PollStatus, Msg> CommandListener::pollForCommand() {
  if (!KafkaSettings.Address.empty() && !CommandTopic.empty()) {
    if (Consumer == nullptr) {
      setUpConsumer();
    } else {
      return Consumer->poll();
    }
  }
  return {Kafka::PollStatus::TimedOut, Msg()};
}

void CommandListener::setUpConsumer() {
  Consumer = _consumer_factory->createConsumer(KafkaSettings);
  if (StartTimestamp < time_point::max()) {
    Consumer->assignAllPartitions(CommandTopic, StartTimestamp);
  } else {
    Consumer->addTopic(CommandTopic);
  }
}

void CommandListener::change_topic(std::string const &new_topic,
                                   time_point start_time) {
  CommandTopic = new_topic;
  StartTimestamp = start_time;
  try_connecting_to_topic();
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
