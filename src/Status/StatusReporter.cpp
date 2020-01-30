// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "StatusReporter.h"
#include "json.h"

namespace Status {

void StatusReporter::start() {
  Logger->trace("Starting the StatusTimer");
  Running = true;
  AsioTimer.async_wait(
      [this](std::error_code const & /*error*/) { this->reportStatus(); });
  StatusThread = std::thread(&StatusReporter::run, this);
}

void StatusReporter::waitForStop() {
  Logger->trace("Stopping StatusTimer");
  IO.stop();
  StatusThread.join();
}

std::string StatusReporter::createReport() const {
  auto Info = nlohmann::json::object();
  std::lock_guard<std::mutex> const lock(StatusMutex);

  Info["update_interval"] = Period.count();
  Info["job_id"] = Status.JobId;
  Info["file_being_written"] = Status.Filename;
  Info["start_time"] = Status.StartTime.count();
  Info["stop_time"] = Status.StopTime.count();

  return Info.dump();
}

void StatusReporter::reportStatus() {
  if (!StatusProducerTopic || !Running) {
    return;
  }

  auto const StatusReport = createReport();
  Logger->debug("status: {}", StatusReport);
  StatusProducerTopic->produce(StatusReport);
  AsioTimer.expires_at(AsioTimer.expires_at() + Period);
  AsioTimer.async_wait(
      [this](std::error_code const & /*error*/) { this->reportStatus(); });
}

StatusReporter::~StatusReporter() { this->waitForStop(); }

void StatusReporter::updateStatusInfo(StatusInfo const &NewInfo) {
  const std::lock_guard<std::mutex> lock(StatusMutex);
  Status = NewInfo;
}

void StatusReporter::updateStopTime(std::chrono::milliseconds StopTime) {
  const std::lock_guard<std::mutex> lock(StatusMutex);
  Status.StopTime = StopTime;
}

void StatusReporter::resetStatusInfo() {
  updateStatusInfo({"", "", std::chrono::milliseconds(0)});
}
} // namespace Status