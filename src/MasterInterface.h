// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CommandListener.h"
#include "KafkaW/ProducerTopic.h"
#include <memory>
#include <string>

struct MainOpt;

namespace FileWriter {

class StreamMaster;
class Streamer;

/// \brief Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class MasterInterface {
public:
  /// \brief Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  virtual void run() = 0;

  virtual void stop() = 0;
  virtual void handle_command(std::unique_ptr<Msg> msg) = 0;
  virtual void handle_command(std::string const &command,
                              std::chrono::milliseconds TimeStamp) = 0;
  virtual void statistics() = 0;

  /// \brief The unique identifier for this file writer on the network.
  ///
  /// \return The unique id.
  virtual std::string getFileWriterProcessId() const = 0;

  virtual bool runLoopExited() = 0;
  virtual MainOpt &getMainOpt() = 0;
  virtual std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() = 0;
};

} // namespace FileWriter
