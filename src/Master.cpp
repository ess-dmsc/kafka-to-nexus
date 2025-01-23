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
  Logger::Info("file-writer service id: {}", Config.getServiceId());
  this->Reporter->setJSONMetaDataGenerator(
      [&](auto &JsonObject) { MetaDataTracker->writeToJSONDict(JsonObject); });
  this->Reporter->setStatusGetter([&]() { return getCurrentStatus(); });
  CurrentStateMetric = static_cast<int64_t>(getCurrentState());
  MasterMetricsRegistrar->registerMetric(CurrentStateMetric,
                                         {Metrics::LogTo::CARBON});
}

void Master::startWriting(Command::StartMessage const &StartInfo) {
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

    std::filesystem::path const filepath =
        construct_filepath(MainConfig.HDFOutputPrefix, StartInfo.Filename);
    std::filesystem::path const template_path = construct_template_path(
        MainConfig.HDFTemplatePrefix, StartInfo.InstrumentName);

    CurrentStreamController = createFileWritingJob(
        StartInfo, streamer_options, filepath, MasterMetricsRegistrar.get(),
        MetaDataTracker, template_path);
    CurrentStreamController->start();

    metadata_from_start_msg = StartInfo.Metadata;
    if (!StartInfo.ControlTopic.empty()) {
      Reporter->useAlternativeStatusTopic(StartInfo.ControlTopic);
    }
    setCurrentStatus({Status::WorkerState::Writing, StartInfo.JobID,
                      StartInfo.Filename, StartInfo.StartTime,
                      StartInfo.StopTime});
  } catch (std::runtime_error const &Error) {
    Logger::Critical("{}", Error.what());
    throw;
  }
}

std::filesystem::path
Master::construct_filepath(std::filesystem::path const &prefix,
                           std::string const &filename) {
  return prefix / std::filesystem::path(filename).relative_path();
}

std::filesystem::path
Master::construct_template_path(std::filesystem::path const &prefix,
                                std::string const &instrument_name) {
  std::filesystem::path local_template_path =
      fmt::format("{0}/{0}.hdf", instrument_name);
  return prefix / local_template_path;
}

void Master::stopNow() {
  if (getCurrentState() != Status::WorkerState::Writing) {
    throw std::runtime_error(
        R"(Unable to stop writing when not in "Writing" state.)");
  }
  Logger::Info("Attempting to stop file-writing (quickly).");
  if (CurrentStreamController != nullptr) {
    CurrentStreamController->stop();
//    CurrentStreamController->setStopTime(system_clock::now());
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
  auto writtenFilePath = getCurrentFilePath();
  if (CurrentStreamController->hasErrorState()) {
    CommandAndControl->sendErrorEncounteredMessage(
        writtenFilePath, metadata_from_start_msg,
        CurrentStreamController->errorMessage());
  } else {
    Logger::Info("Full path of file written: {}", writtenFilePath.string());
    CommandAndControl->sendHasStoppedMessage(writtenFilePath,
                                             metadata_from_start_msg);
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
