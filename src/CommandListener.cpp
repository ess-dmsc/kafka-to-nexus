// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <KafkaW/ConsumerFactory.h>
#include <memory>

#include "CommandListener.h"
#include "KafkaW/PollStatus.h"
#include "Msg.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {

using std::string;

CommandListener::CommandListener(MainOpt &config) : config(config) {}

void CommandListener::start() {
  KafkaW::BrokerSettings BrokerSettings =
      config.StreamerConfiguration.BrokerSettings;
  BrokerSettings.PollTimeoutMS = 500;
  BrokerSettings.Address = config.CommandBrokerURI.HostPort;
  consumer = KafkaW::createConsumer(BrokerSettings, BrokerSettings.Address);
  if (consumer->topicPresent(config.CommandBrokerURI.Topic))
    consumer->addTopic(config.CommandBrokerURI.Topic);
  else {
    Logger->error(
        "Topic {} not in broker. Could not start listener for topic {}.",
        config.CommandBrokerURI.Topic, config.CommandBrokerURI.Topic);
    throw std::runtime_error(fmt::format(
        "Topic {} not in broker. Could not start listener for topic {}.",
        config.CommandBrokerURI.Topic, config.CommandBrokerURI.Topic));
  }
}

std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> CommandListener::poll() {
  return consumer->poll();
}

} // namespace FileWriter
