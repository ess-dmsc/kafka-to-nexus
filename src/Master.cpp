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
               std::unique_ptr<Metrics::IRegistrar> Registrar)
    : MainConfig(Config), CommandAndControl(std::move(Listener)),
      Reporter(std::move(Reporter)),
      MasterMetricsRegistrar(std::move(Registrar)) {
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
  CurrentStateMetric = static_cast<int64_t>(getCurrentState());
  MasterMetricsRegistrar->registerMetric(CurrentStateMetric,
                                         {Metrics::LogTo::CARBON});
}

void Master::startWriting(Command::StartInfo const &StartInfo) {
  if (getCurrentState() == Status::WorkerState::Writing) {
    throw std::runtime_error(fmt::format(
        R"(Unable to start new writing job (with id: "{}") when waiting for the current one to finish.)",
        StartInfo.JobID));
  }
  try {
    MetaDataTracker->clearMetaData();

    StreamerOptions streamer_options = MainConfig.StreamerConfiguration;
    streamer_options.StartTimestamp = StartInfo.StartTime;
    streamer_options.StopTimestamp = StartInfo.StopTime;
    streamer_options.BrokerSettings.Address = MainConfig.JobPoolURI.HostPort;

    std::filesystem::path const filepath =
        std::filesystem::path(MainConfig.HDFOutputPrefix) /
        std::filesystem::path(StartInfo.Filename).relative_path();

    CurrentStreamController =
        createFileWritingJob(StartInfo, streamer_options, filepath,
                             MasterMetricsRegistrar.get(), MetaDataTracker);
    CurrentMetadata = StartInfo.Metadata;
    if (!StartInfo.ControlTopic.empty()) {
      Reporter->useAlternativeStatusTopic(StartInfo.ControlTopic);
    }
    setCurrentStatus({Status::WorkerState::Writing, StartInfo.JobID,
                      StartInfo.Filename, StartInfo.StartTime,
                      StartInfo.StopTime});
  } catch (std::runtime_error const &Error) {
    LOG_CRITICAL("{}", Error.what());
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
    CurrentStreamController->setStopTime(system_clock::now());
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
        nlohmann::json::parse(Reporter->createJSONReport().dump());
    auto StaticMetaData = nlohmann::json::object();
    try {
      StaticMetaData = nlohmann::json::parse(CurrentMetadata);
    } catch (nlohmann::json::parse_error const &E) {
      LOG_WARN("Failed to parse JSON metadata string from start message. "
               "Skipping (metadata={})",
               CurrentMetadata);
    }
    CurrentJSONStatus.update(StaticMetaData);
    auto writtenFilePath = getCurrentFilePath();
    LOG_INFO("Full path of file written: {} (filename from job: {})",
             writtenFilePath.string(), getCurrentFileName());
    CommandAndControl->sendHasStoppedMessage(writtenFilePath,
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

const Metrics::Metric &Master::getCurrentStateMetric() const {
  std::lock_guard LockGuard(StatusMutex);
  return CurrentStateMetric;
}

std::string Master::getCurrentFileName() const {
  std::lock_guard LockGuard(StatusMutex);
  return CurrentStatus.Filename;
}

std::filesystem::path Master::getCurrentFilePath() const {
  std::lock_guard LockGuard(StatusMutex);
  return std::filesystem::path(MainConfig.getHDFOutputPrefix()) /
         std::filesystem::path(CurrentStatus.Filename).relative_path();
}

void Master::setStopTimeInternal(time_point NewStopTime) {
  std::lock_guard LockGuard(StatusMutex);
  CurrentStatus.StopTime = NewStopTime;
}

void Master::setCurrentStatus(Status::JobStatusInfo const &NewStatus) {
  std::lock_guard LockGuard(StatusMutex);
  CurrentStatus = NewStatus;
  CurrentStateMetric = static_cast<int64_t>(CurrentStatus.State);
}

void Master::resetStatusInfo() {
  std::lock_guard LockGuard(StatusMutex);
  CurrentStatus = {};
  CurrentStateMetric = static_cast<int64_t>(CurrentStatus.State);
}

} // namespace FileWriter
