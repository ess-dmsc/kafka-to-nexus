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

namespace Command {

CommandListener::CommandListener(uri::URI CommandTopicUri,
                                 Kafka::BrokerSettings Settings)
    : KafkaAddress(CommandTopicUri.HostPort),
      CommandTopic(CommandTopicUri.Topic), KafkaSettings(Settings),
      CurrentTimeOut(Settings.MinMetadataTimeout) {
  KafkaSettings.Address = CommandTopicUri.HostPort;
}

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
  try {
    auto Partitions = Kafka::getPartitionsForTopic(KafkaAddress, CommandTopic,
                                                   CurrentTimeOut);
    Consumer = Kafka::createConsumer(KafkaSettings);
    for (auto const &PartitionId : Partitions) {
      Consumer->addPartitionAtOffset(CommandTopic, PartitionId,
                                     RdKafka::Topic::OFFSET_END);
    }
  } catch (MetadataException const &E) {
    CurrentTimeOut *= 2;
    if (CurrentTimeOut > KafkaSettings.MaxMetadataTimeout) {
      CurrentTimeOut = KafkaSettings.MaxMetadataTimeout;
    }
    LOG_WARN(
        "Meta data call for retrieving partition IDs for topic \"{}\" "
        "from the broker "
        "failed. The failure message was: \"{}\". Re-trying with a "
        "timeout of {} ms.",
        CommandTopic, E.what(),
        std::chrono::duration_cast<std::chrono::milliseconds>(CurrentTimeOut)
            .count());
  }
}

} // namespace Command
