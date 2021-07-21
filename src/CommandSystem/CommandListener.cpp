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
      CommandTopic(CommandTopicUri.Topic), KafkaSettings(Settings) {
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
  Consumer = Kafka::createConsumer(KafkaSettings);
  Consumer->addTopic(CommandTopic);
}

} // namespace Command
