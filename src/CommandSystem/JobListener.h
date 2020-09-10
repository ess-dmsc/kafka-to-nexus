// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CommandSystem/CommandListener.h"
#include "Kafka/Consumer.h"
#include "MainOpt.h"
#include "Msg.h"
#include "logger.h"

namespace Command {

using FileWriter::Msg;

/// Check for new commands on the topic, return them to the Master.
class JobListener : private CommandListener {
public:
  JobListener(uri::URI JobPoolUri, Kafka::BrokerSettings Settings);

  std::pair<Kafka::PollStatus, Msg> pollForJob();

  void disconnectFromPool();

private:
  // Do not change the ConsumerGroupId variable, it is vital to the workings of
  // the worker pool
  std::string const ConsumerGroupId{"kafka-to-nexus-worker-pool"};
};
} // namespace Command
