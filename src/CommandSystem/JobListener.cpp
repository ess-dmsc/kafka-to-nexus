// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <Kafka/ConsumerFactory.h>

#include <utility>

#include "JobListener.h"
#include "Kafka/PollStatus.h"
#include "Msg.h"

namespace Command {

using std::string;

JobListener::JobListener(
    std::string const &job_pool_topic, Kafka::BrokerSettings const &settings,
    const std::shared_ptr<Kafka::ConsumerFactoryInterface> &consumer_factory)
    : CommandListener(job_pool_topic, settings, consumer_factory) {
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
