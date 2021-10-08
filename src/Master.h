// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CommandSystem/Handler.h"
#include "Kafka/PollStatus.h"
#include "MainOpt.h"
#include "MetaData/Tracker.h"
#include "Metrics/Registrar.h"
#include "Msg.h"
#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace Status {
class StatusReporterBase;
}

namespace FileWriter {
class IStreamController;

/// \brief Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
  Master(MainOpt &Config, std::unique_ptr<Command::HandlerBase> Listener,
         std::unique_ptr<Status::StatusReporterBase> Reporter,
         Metrics::Registrar const &Registrar);
  virtual ~Master() = default;

  /// \brief Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run();

  void setStopTime(time_point StopTime);
  void stopNow();
  void startWriting(Command::StartInfo const &StartInfo);

private:
  enum class WriterState { Idle, Writing };
  MainOpt &MainConfig;
  std::unique_ptr<Command::HandlerBase> CommandAndControl;
  std::unique_ptr<IStreamController> CurrentStreamController{nullptr};
  std::unique_ptr<Status::StatusReporterBase> Reporter;
  Metrics::Registrar MasterMetricsRegistrar;
  WriterState CurrentState{WriterState::Idle};
  std::string CurrentFileName;
  std::string CurrentMetadata;
  MetaData::TrackerPtr MetaDataTracker{std::make_shared<MetaData::Tracker>()};
  virtual bool hasWritingStopped();
  void setToIdle();
};
} // namespace FileWriter
