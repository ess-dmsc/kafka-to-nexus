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
#include "MainOpt.h"
#include "MasterInterface.h"
#include "StreamMaster.h"
#include "Streamer.h"
#include <atomic>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace FileWriter {

/// \brief Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master : public MasterInterface {
public:
  explicit Master(MainOpt &Config);

  /// \brief Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run() override;

  /// Stop running.
  void stop() override;
  void handle_command_message(std::unique_ptr<Msg> CommandMessage) override;
  void handle_command(std::string const &Command) override;
  void statistics() override;
  void addStreamMaster(
      std::unique_ptr<StreamMaster> StreamMaster) override;
  void stopStreamMasters() override;
  std::unique_ptr<StreamMaster> &
  getStreamMasterForJobID(std::string const &JobID) override;
  MainOpt &getMainOpt() override;
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() override;

  /// \brief The unique identifier for this file writer on the network.
  ///
  /// \return The unique id.
  std::string getFileWriterProcessId() const override;

  bool runLoopExited() override { return HasExitedRunLoop; };
  std::shared_ptr<KafkaW::ProducerTopic> StatusProducer;

private:
  SharedLogger Logger;
  CommandListener Listener;
  std::atomic<bool> Running{true};
  std::atomic<bool> HasExitedRunLoop{false};
  std::vector<std::unique_ptr<StreamMaster>> StreamMasters;
  std::string FileWriterProcessId;
  MainOpt &MainConfig;
};
} // namespace FileWriter
