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

void StatusReporterBase::updateStatusInfo(StatusInfo const &NewInfo) {
  const std::lock_guard<std::mutex> lock(StatusMutex);
  Status = NewInfo;
}

void StatusReporterBase::updateStopTime(std::chrono::milliseconds StopTime) {
  const std::lock_guard<std::mutex> lock(StatusMutex);
  Status.StopTime = time_point(StopTime);
}

void StatusReporterBase::resetStatusInfo() {
  updateStatusInfo({"", "", std::chrono::milliseconds(0)});
}

Kafka::ProducerMessage
StatusReporterBase::createReport(std::string const &JSONReport) const {
  std::lock_guard<std::mutex> const lock(StatusMutex);

  // Info["update_interval"] = Period.count();
  UNUSED_ARG(JSONReport);

  return {};
}

// Create the JSON part of the status message
std::string StatusReporterBase::createJSONReport() const {
  auto Info = nlohmann::json::object();
  std::lock_guard<std::mutex> const lock(StatusMutex);

  Info["job_id"] = Status.JobId;
  Info["service_id"] = ServiceIdentifier;
  Info["file_being_written"] = Status.Filename;
  Info["start_time"] = Status.StartTime.count();
  Info["stop_time"] = toMilliSeconds(Status.StopTime);

  return Info.dump();
}

void StatusReporterBase::reportStatus() {
  if (!StatusProducerTopic) {
    return;
  }

  auto const StatusJSONReport = createJSONReport();
  Logger->debug("status: {}", StatusJSONReport);

  auto StatusReportMessage =
      std::make_unique<Kafka::ProducerMessage>(createReport(StatusJSONReport));
  StatusProducerTopic->produce(std::move(StatusReportMessage));
  postReportStatusActions();
}

} // namespace Status
