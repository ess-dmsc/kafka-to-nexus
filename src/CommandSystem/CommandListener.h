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
#include "Msg.h"
#include "TimeUtility.h"
#include "logger.h"

namespace Command {

using FileWriter::Msg;

/// \brief Check for new commands on a command listener topic.
class CommandListener {
public:
  /// \brief Constructor without specific StartTimestamp.
  ///
  /// \param CommandTopicUri The URI/URL of the Kafka broker + topic to connect
  /// to for new commands. \param Settings Kafka (consumer) settings.
  CommandListener(uri::URI CommandTopicUri, Kafka::BrokerSettings Settings);

  /// \brief Constructor with specific StartTimestamp.
  ///
  /// \param CommandTopicUri The URI/URL of the Kafka broker + topic to connect
  /// to for new commands. \param Settings Kafka (consumer) settings. \param
  /// StartTimestamp Point in time to start listening for commands.
  CommandListener(uri::URI CommandTopicUri, Kafka::BrokerSettings Settings,
                  time_point StartTimestamp);

  /// \brief Destructor.
  virtual ~CommandListener() = default;

  /// \brief Poll the Kafka topic for a new command.
  ///
  /// \note Will timeout on its first call. Will continue to timeout as long
  /// as it fails to retrieve metadata from the Kafka broker
  ///
  /// \return Get a std::pair<> that contains the outcome of the message
  /// poll and the message if one was successfully received.
  virtual std::pair<Kafka::PollStatus, Msg> pollForCommand();

protected:
  std::string const KafkaAddress;
  std::string const CommandTopic;
  Kafka::BrokerSettings KafkaSettings;
  void setUpConsumer();
  std::unique_ptr<Kafka::ConsumerInterface> Consumer;
  time_point StartTimestamp = time_point::max();
};
} // namespace Command
