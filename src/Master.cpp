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
#include "logger.h"
#include <chrono>
#include <functional>

namespace FileWriter {

Master::Master(MainOpt &Config, std::unique_ptr<Command::HandlerBase> Listener,
               std::unique_ptr<Status::StatusReporterBase> Reporter,
               Metrics::Registrar const &Registrar)
    : MainConfig(Config), CommandAndControl(std::move(Listener)),
      Reporter(std::move(Reporter)), MasterMetricsRegistrar(Registrar) {
  CommandAndControl->registerGetJobIdFunction(
      [&]() { return this->getCurrentStatus().JobId; });
  CommandAndControl->registerStartFunction(
      [this](auto StartInfo) { this->startWriting(StartInfo); });
  CommandAndControl->registerSetStopTimeFunction(
      [this](auto StopTime) { this->setStopTime(StopTime); });
  CommandAndControl->registerStopNowFunction([this]() { this->stopNow(); });
  CommandAndControl->registerIsWritingFunction(
      [this]() { return Status::WorkerState::Writing == getCurrentState(); });
  LOG_INFO("file-writer service id: {}", Config.getServiceId());
  this->Reporter->setJSONMetaDataGenerator(
      [&](auto &JsonObject) { MetaDataTracker->writeToJSONDict(JsonObject); });
  this->Reporter->setStatusGetter([&]() { return getCurrentStatus(); });
}

void Master::startWriting(Command::StartInfo const &StartInfo) {
  if (getCurrentState() == Status::WorkerState::Writing) {
    throw std::runtime_error(fmt::format(
        R"(Unable to start new writing job (with id: "{}") when waiting for the current one to finish.)",
        StartInfo.JobID));
  }
  try {
    MetaDataTracker->clearMetaData();
    CurrentStreamController = createFileWritingJob(
        StartInfo, MainConfig, MasterMetricsRegistrar, MetaDataTracker);
    CurrentMetadata = StartInfo.Metadata;
    ;
    if (not StartInfo.ControlTopic.empty()) {
      Reporter->useAlternativeStatusTopic(StartInfo.ControlTopic);
    }
    setCurrentStatus({Status::WorkerState::Writing, StartInfo.JobID,
                      StartInfo.Filename, StartInfo.StartTime,
                      StartInfo.StopTime});
  } catch (std::runtime_error const &Error) {
    LOG_ERROR("{}", Error.what());
    throw;
  }
}

void Master::stopNow() {
  if (getCurrentState() != Status::WorkerState::Writing) {
    throw std::runtime_error(
        R"(Unable to stop writing when not in "Writing" state.)");
  }
  LOG_INFO("Attempting to stop file-writing (quickly).");
  if (CurrentStreamController != nullptr) {
    CurrentStreamController->stop();
  }
  setStopTimeInternal(time_point{0ms});
}

void Master::setStopTime(time_point NewStopTime) {
  if (getCurrentState() != Status::WorkerState::Writing) {
    throw std::runtime_error(
        R"(Unable to set stop time when not in "Writing" state.)");
  }
  if (Reporter->getStopTime() < system_clock::now()) {
    throw std::runtime_error("Unable to set a new stop time as the stop time "
                             "has already been passed.");
  }
  CurrentStreamController->setStopTime(NewStopTime);
  setStopTimeInternal(NewStopTime);
}

bool Master::hasWritingStopped() {
  return CurrentStreamController != nullptr and
         CurrentStreamController->isDoneWriting();
}

bool Master::writingIsFinished() {
  return CurrentStreamController == nullptr or
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
        getCurrentFileName(), CurrentMetadata,
        CurrentStreamController->errorMessage());
  } else {
    auto CurrentJSONStatus =
        nlohmann::json::parse(Reporter->createJSONReport());
    auto StaticMetaData = nlohmann::json::object();
    try {
      StaticMetaData = nlohmann::json::parse(CurrentMetadata);
    } catch (nlohmann::json::parse_error const &E) {
      LOG_WARN(
          "Failed to parse JSON metadata string from start message. Skipping.");
    }
    CurrentJSONStatus.update(StaticMetaData);
    CommandAndControl->sendHasStoppedMessage(getCurrentFilePath(),
                                             CurrentJSONStatus);
  }
  CurrentStreamController.reset(nullptr);
  MetaDataTracker->clearMetaData();
  resetStatusInfo();
  Reporter->revertToDefaultStatusTopic();
}

time_point Master::getStopTime() const {
  std::lock_guard LockGuard(StatusMutex);
  return CurrentStatus.StopTime;
}

Status::JobStatusInfo Master::getCurrentStatus() const {
  std::lock_guard LockGuard(StatusMutex);
  return CurrentStatus;
}

Status::WorkerState Master::getCurrentState() const {
  std::lock_guard LockGuard(StatusMutex);
  return CurrentStatus.State;
}

std::string Master::getCurrentFileName() const {
  std::lock_guard LockGuard(StatusMutex);
  return CurrentStatus.Filename;
}

std::string Master::getCurrentFilePath() const {
  std::lock_guard LockGuard(StatusMutex);
  std::string fullFilePath = MainConfig.getHDFOutputPrefix();
  if (fullFilePath.empty()) {
    fullFilePath = CurrentStatus.Filename;
  } else {
    fullFilePath = fullFilePath + "/" + CurrentStatus.Filename;
  }
  return fullFilePath;
}

void Master::setStopTimeInternal(time_point NewStopTime) {
  std::lock_guard LockGuard(StatusMutex);
  CurrentStatus.StopTime = NewStopTime;
}

void Master::setCurrentStatus(Status::JobStatusInfo const &NewStatus) {
  std::lock_guard LockGuard(StatusMutex);
  CurrentStatus = NewStatus;
}

void Master::resetStatusInfo() {
  std::lock_guard LockGuard(StatusMutex);
  CurrentStatus = {};
}

} // namespace FileWriter
