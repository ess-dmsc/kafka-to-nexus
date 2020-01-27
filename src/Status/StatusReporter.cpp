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

void StatusReporter::reportStatus() {
  if (!StatusProducerTopic || !Running) {
    return;
  }
  using nlohmann::json;
  auto Info = json::object();

  Info["update_interval"] = Period.count();
  Info["number_of_events_written"] = -1;
  {
    const std::lock_guard<std::mutex> lock(StatusMutex);
    Info["job_id"] = Status.jobId;
    Info["file_being_written"] = Status.filename;
    Info["start_time"] = Status.startTime.count();
  }

  auto StatusString = Info.dump();
  auto StatusStringSize = StatusString.size();
  if (StatusStringSize > 1000) {
    auto StatusStringShort =
        StatusString.substr(0, 1000) +
            fmt::format(" ... {} chars total ...", StatusStringSize);
    Logger->debug("status: {}", StatusStringShort);
  } else {
    Logger->debug("status: {}", StatusString);
  }
  StatusProducerTopic->produce(StatusString);
  AsioTimer.expires_at(AsioTimer.expires_at() + Period);
  AsioTimer.async_wait(
      [this](std::error_code const & /*error*/) { this->reportStatus(); });
}

StatusReporter::~StatusReporter() { this->waitForStop(); }
} // namespace Status