// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CommandParser.h"
#include "Kafka/PollStatus.h"
#include "MainOpt.h"
#include "Msg.h"
#include "States.h"
#include <atomic>
#include <memory>
#include <mpark/variant.hpp>
#include <string>
#include <vector>

namespace Status {
class StatusReporter;
}

namespace FileWriter {
class IJobCreator;
class CommandListener;
class IStreamController;

FileWriterState getNextState(Msg const &Command,
                             std::chrono::milliseconds TimeStamp,
                             FileWriterState const &CurrentState);

/// \brief Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
  Master(MainOpt &Config, std::unique_ptr<CommandListener> Listener,
         std::unique_ptr<IJobCreator> Creator,
         std::unique_ptr<Status::StatusReporter> Reporter);
  virtual ~Master() = default;

  /// \brief Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run();

  bool isWriting() const;

private:
  SharedLogger Logger;
  MainOpt &MainConfig;
  std::unique_ptr<CommandListener> CmdListener;
  std::unique_ptr<IJobCreator> Creator_;
  std::unique_ptr<IStreamController> CurrentStreamController{nullptr};
  std::unique_ptr<Status::StatusReporter> Reporter;
  FileWriterState CurrentState = States::Idle();
  virtual void startWriting(StartCommandInfo const &StartInfo);
  virtual void requestStopWriting(StopCommandInfo const &StopInfo);
  virtual bool hasWritingStopped();
  virtual std::pair<Kafka::PollStatus, Msg> pollForMessage();
  virtual void moveToNewState(FileWriterState const &NewState);
  virtual FileWriterState handleCommand(Msg const &CommandMessage);
  void setToIdle();
};
} // namespace FileWriter
