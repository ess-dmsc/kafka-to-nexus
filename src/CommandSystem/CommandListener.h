// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <utility>

#include "Kafka/Consumer.h"
#include "Kafka/ConsumerFactory.h"
#include "MainOpt.h"
#include "Msg.h"
#include "TimeUtility.h"
#include "logger.h"

namespace Command {

using FileWriter::Msg;

/// \brief Check for new commands on a command listener topic.
class CommandListener {
public:
  /// \brief Constructor without specific start_timestamp.
  ///
  /// \param command_topic_uri The URI/URL of the Kafka broker + topic to
  /// connect to for new commands.
  /// \param settings Kafka (consumer) settings.
  /// \param consumer_factory Factory for creating consumers.
  CommandListener(
      uri::URI const &command_topic_uri, Kafka::BrokerSettings settings,
      std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory =
          std::make_shared<Kafka::ConsumerFactory>());

  /// \brief Constructor with specific start_timestamp.
  ///
  /// connect to for new commands.
  /// \param settings Kafka (consumer) settings.
  /// \param start_timestamp Point in time to start listening for commands.
  /// \param consumer_factory Factory for creating consumers.
  CommandListener(
      uri::URI const &command_topic_uri, Kafka::BrokerSettings settings,
      time_point start_timestamp,
      std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory =
          std::make_shared<Kafka::ConsumerFactory>());

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

  /// \brief Try changing to the specified topic.
  ///
  /// Will throw an exception if it cannot connect.
  void change_topic(std::string const &new_topic,
                    time_point start_time = time_point::max());

protected:
  std::string const KafkaAddress;
  std::string CommandTopic;
  Kafka::BrokerSettings KafkaSettings;
  void setUpConsumer();
  time_point StartTimestamp = time_point::max();
  std::shared_ptr<Kafka::ConsumerInterface> Consumer;

private:
  void try_connecting_to_topic();

  std::shared_ptr<Kafka::ConsumerFactoryInterface> _consumer_factory;
};
} // namespace Command
