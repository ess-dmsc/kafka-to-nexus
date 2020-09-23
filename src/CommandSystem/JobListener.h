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

/// \brief Check for new jobs on a topic.
class JobListener : private CommandListener {
public:
  /// \brief The constructor will not automatically connect to the Kafka broker.
  JobListener(uri::URI JobPoolUri, Kafka::BrokerSettings Settings);

  /// \brief Poll the Kafka topic for a new job.
  ///
  /// If we are currently not connected to the Kafka broker (for whatever
  /// reason), this function will try to connect. This will always result in a
  /// timeout regardless of if the connection attempt was successfull.
  std::pair<Kafka::PollStatus, Msg> pollForJob();

  /// \brief Disconnect from the Kafka broker (topic) to prevent the consumer
  /// from receiving (job) messages that it will ignore.
  ///
  /// This function should (probably) be called after a successfull call to
  /// pollForJob().
  void disconnectFromPool();

private:
  // Do not change the ConsumerGroupId variable, it is vital to the workings of
  // the worker pool
  std::string const ConsumerGroupId{"kafka-to-nexus-worker-pool"};
};
} // namespace Command
