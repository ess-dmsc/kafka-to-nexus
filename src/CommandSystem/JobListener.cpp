// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <Kafka/ConsumerFactory.h>

#include "JobListener.h"
#include "Kafka/PollStatus.h"
#include "Msg.h"

namespace Command {

using std::string;

JobListener::JobListener(uri::URI JobPoolUri, Kafka::BrokerSettings Settings)
    : CommandListener(JobPoolUri, Settings) {
  KafkaSettings.KafkaConfiguration["group.id"] = ConsumerGroupId;
  KafkaSettings.KafkaConfiguration["queued.min.messages"] = "1";
  KafkaSettings.KafkaConfiguration["enable.auto.commit"] = "true";
}

std::pair<Kafka::PollStatus, Msg> JobListener::pollForJob() {
  return pollForCommand();
}

void JobListener::disconnectFromPool() { Consumer.reset(); }
bool JobListener::isConnected() const { return Consumer != nullptr; }

} // namespace Command
