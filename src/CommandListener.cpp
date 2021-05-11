// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <Kafka/ConsumerFactory.h>

#include "CommandListener.h"
#include "Kafka/PollStatus.h"
#include "Msg.h"

namespace FileWriter {

using std::string;

CommandListener::CommandListener(MainOpt &Config) : config(Config) {}

void CommandListener::start() {
  Kafka::BrokerSettings BrokerSettings =
      config.StreamerConfiguration.BrokerSettings;
  BrokerSettings.Address = config.CommandBrokerURI.HostPort;
    Consumer =  Kafka::createConsumer(BrokerSettings, BrokerSettings.Address);
  Consumer->addTopic(config.CommandBrokerURI.Topic);
}

std::pair<Kafka::PollStatus, Msg> CommandListener::poll() {
  return Consumer->poll();
}

} // namespace FileWriter
