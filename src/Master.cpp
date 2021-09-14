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
               std::unique_ptr<Status::StatusReporter> Reporter,
               Metrics::Registrar const &Registrar)
    : MainConfig(Config),
      CommandAndControl(std::move(Listener)), Reporter(std::move(Reporter)),
      MasterMetricsRegistrar(Registrar) {
  CommandAndControl->registerStartFunction(
      [this](auto StartInfo) { this->startWriting(StartInfo); });
  CommandAndControl->registerSetStopTimeFunction(
      [this](auto StopTime) { this->setStopTime(StopTime); });
  CommandAndControl->registerStopNowFunction([this]() { this->stopNow(); });
  CommandAndControl->registerIsWritingFunction(
      [this]() { return WriterState::Writing == CurrentState; });
  LOG_INFO("file-writer service id: {}", Config.getServiceId());
  this->Reporter->setJSONMetaDataGenerator(
      [&](auto &JsonObject) { MetaDataTracker->writeToJSONDict(JsonObject); });
}

void Master::startWriting(Command::StartInfo const &StartInfo) {
  if (CurrentState == WriterState::Writing) {
    throw std::runtime_error(fmt::format(
        "Unable to start new writing job (with id: \"{}\") when waiting for "
        "the current one to finish.",
        StartInfo.JobID));
  }
  try {
    MetaDataTracker->clearMetaData();
    CurrentStreamController = createFileWritingJob(
        StartInfo, MainConfig, MasterMetricsRegistrar, MetaDataTracker);
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
    LOG_ERROR("{}", Error.what());
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
  MetaDataTracker->clearMetaData();
  Reporter->resetStatusInfo();
  Reporter->revertToDefaultStatusTopic();
}

} // namespace FileWriter
