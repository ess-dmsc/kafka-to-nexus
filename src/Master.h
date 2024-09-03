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
#include "Status/StatusInfo.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace Status {
class StatusReporterBase;
}

namespace FileWriter {
class StreamController;

/// \brief Listens to the Kafka configuration topic and handles any requests.
///
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
  Master(MainOpt &Config, std::unique_ptr<Command::HandlerBase> Listener,
         std::unique_ptr<Status::StatusReporterBase> Reporter,
         std::unique_ptr<Metrics::IRegistrar> Registrar);
  virtual ~Master() = default;

  /// \brief Sets up command listener and handles any commands received.
  ///
  /// Continues running until stop requested.
  void run();

  void setStopTime(time_point NewStopTime);
  time_point getStopTime() const;
  Status::JobStatusInfo getCurrentStatus() const;
  Status::WorkerState getCurrentState() const;
  const Metrics::Metric &getCurrentStateMetric() const;
  std::filesystem::path getCurrentFilePath() const;
  std::filesystem::path construct_filepath(std::filesystem::path const &prefix,
                                           std::string const &filename);
  std::filesystem::path
  construct_template_path(std::filesystem::path const &prefix,
                          std::string const &instrument_name);
  virtual void stopNow();
  void startWriting(Command::StartMessage const &StartInfo);
  virtual bool writingIsFinished();

private:
  void setStopTimeInternal(time_point NewStopTime);
  void setCurrentStatus(Status::JobStatusInfo const &NewStatus);
  void resetStatusInfo();
  MainOpt &MainConfig;
  std::unique_ptr<Command::HandlerBase> CommandAndControl;
  std::unique_ptr<StreamController> CurrentStreamController{nullptr};
  std::unique_ptr<Status::StatusReporterBase> Reporter;
  std::unique_ptr<Metrics::IRegistrar> MasterMetricsRegistrar;
  mutable std::mutex StatusMutex;
  Status::JobStatusInfo CurrentStatus;
  Metrics::Metric CurrentStateMetric{"worker_state", "idle/writing"};
  std::string metadata_from_start_msg;
  MetaData::TrackerPtr MetaDataTracker{std::make_shared<MetaData::Tracker>()};
  void setToIdle();
  virtual bool hasWritingStopped();
};
} // namespace FileWriter
