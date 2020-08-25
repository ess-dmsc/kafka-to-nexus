// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "Kafka/Consumer.h"
#include "MainOpt.h"
#include "logger.h"
#include "Msg.h"

namespace Command {

using FileWriter::Msg;

/// Check for new commands on the topic, return them to the Master.
class JobListener {
public:
  explicit JobListener(uri::URI JobPoolUri);

  virtual std::pair<Kafka::PollStatus, Msg> pollForJob();

  void disconnectFromPool();

private:
  // Do not change the ConsumerGroupId variable, it is vital to the working of the 
  std::string const ConsumerGroupId{"kafka-to-nexus-worker-pool"};
  std::string const BrokerAddress;
  std::string const PoolTopic;
  BrokerSettings
  void setUpConsumer();
  std::unique_ptr<Kafka::ConsumerInterface> Consumer;
};
} // namespace Command
