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

#include <flatbuffers/flatbuffers.h>

namespace FlatBuffer {
#include <x5f2_status_generated.h>
}

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

flatbuffers::DetachedBuffer
StatusReporterBase::createReport(std::string const &JSONReport) const {
  std::lock_guard<std::mutex> const lock(StatusMutex);

  flatbuffers::FlatBufferBuilder Builder;

  auto SoftwareName = Builder.CreateString("SOFTWARE_NAME");
  auto SoftwareVersion = Builder.CreateString("SOFTWARE_VERSION");
  auto ServiceId = Builder.CreateString("SERVICE_ID");
  auto HostName = Builder.CreateString("HOST_NAME");
  uint32_t ProcessId = 0;
  uint32_t UpdateInterval = Period.count();
  auto JSONStatus = Builder.CreateString(JSONReport);

  auto MsgBuffer = FlatBuffer::CreateStatus(Builder, SoftwareName, SoftwareVersion, ServiceId, HostName, ProcessId, UpdateInterval, JSONStatus);
  FlatBuffer::FinishStatusBuffer(Builder, MsgBuffer);
  return Builder.Release();
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

  StatusProducerTopic->produce(createReport(StatusJSONReport));
  postReportStatusActions();
}

} // namespace Status
