// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Master.h"
#include "JobCreator.h"
#include "Status/StatusReporter.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <functional>
#include <variant>

namespace FileWriter {

Master::Master(MainOpt &Config, std::unique_ptr<Command::Handler> Listener,
               std::unique_ptr<IJobCreator> Creator,
               std::unique_ptr<Status::StatusReporter> Reporter,
               Metrics::Registrar const &Registrar)
    : Logger(getLogger()), MainConfig(Config),
      CommandAndControl(std::move(Listener)), Creator_(std::move(Creator)),
      Reporter(std::move(Reporter)), MasterMetricsRegistrar(Registrar) {
  CommandAndControl->registerStartFunction(
      [this](auto StartInfo) { this->startWriting(StartInfo); });
  CommandAndControl->registerSetStopTimeFunction(
      [this](auto StopTime) { this->setStopTime(StopTime); });
  CommandAndControl->registerStopNowFunction([this]() { this->stopNow(); });
  Logger->info("file-writer service id: {}", Config.getServiceId());
}

void Master::startWriting(Command::StartInfo const &StartInfo) {
  if (CurrentState == WriterState::Writing) {
    throw std::runtime_error("Unable to start new writing job when waiting for "
                             "the current one to finish.");
  }
  try {
    CurrentStreamController = Creator_->createFileWritingJob(
        StartInfo, MainConfig, Logger, MasterMetricsRegistrar);
    CurrentFileName = StartInfo.Filename;
    CurrentMetadata = StartInfo.Metadata;
    CurrentState = WriterState::Writing;
    if (not StartInfo.ControlTopic.empty()) {
      Reporter->useAlternativeStatusTopic(StartInfo.ControlTopic);
    }
    Reporter->updateStatusInfo({Status::JobStatusInfo::WorkerState::Writing,
                                StartInfo.JobID, StartInfo.Filename,
                                StartInfo.StartTime, StartInfo.StopTime});
  } catch (std::runtime_error const &Error) {
    Logger->error("{}", Error.what());
    throw;
  }
}

void Master::stopNow() {
  if (CurrentState != WriterState::Writing) {
    throw std::runtime_error(
        "Unable to stop writing when not in \"Writing\" state.");
  }
  LOG_INFO("Attempting to stop file-writing (quickly).");
  CurrentStreamController->stop();
  Reporter->updateStopTime(time_point{0ms});
}

void Master::setStopTime(time_point StopTime) {
  if (CurrentState != WriterState::Writing) {
    throw std::runtime_error(
        "Unable to set stop time when not in \"Writing\" state.");
  }
  CurrentStreamController->setStopTime(StopTime);
  Reporter->updateStopTime(StopTime);
}

bool Master::hasWritingStopped() {
  return CurrentStreamController != nullptr and
         CurrentStreamController->isDoneWriting();
}

void Master::run() {
  CommandAndControl->loopFunction();
  // Handle error case
  if (hasWritingStopped()) {
    setToIdle();
  }
}

void Master::setToIdle() {
  if (CurrentStreamController->hasErrorState()) {
    CommandAndControl->sendErrorEncounteredMessage(
        CurrentFileName, CurrentMetadata,
        CurrentStreamController->errorMessage());
  } else {
    CommandAndControl->sendHasStoppedMessage(CurrentFileName, CurrentMetadata);
  }
  CurrentStreamController.reset(nullptr);
  CurrentState = WriterState::Idle;
  Reporter->resetStatusInfo();
  Reporter->revertToDefaultStatusTopic();
}

} // namespace FileWriter
