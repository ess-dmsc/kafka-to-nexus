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

void StatusReporterBase::updateStatusInfo(JobStatusInfo const &NewInfo) {
  const std::lock_guard<std::mutex> lock(StatusMutex);
  Status = NewInfo;
}

void StatusReporterBase::updateStopTime(std::chrono::milliseconds StopTime) {
  const std::lock_guard<std::mutex> lock(StatusMutex);
  Status.StopTime = time_point(StopTime);
}

void StatusReporterBase::resetStatusInfo() {
  updateStatusInfo(
      {JobStatusInfo::WorkerState::Idle, "", "", std::chrono::milliseconds(0)});
}

flatbuffers::DetachedBuffer
StatusReporterBase::createReport(std::string const &JSONReport) const {
  std::lock_guard<std::mutex> const lock(StatusMutex);

  flatbuffers::FlatBufferBuilder Builder;

  auto SoftwareName =
      Builder.CreateString(StaticStatusInformation.ApplicationName);
  auto SoftwareVersion =
      Builder.CreateString(StaticStatusInformation.ApplicationVersion);
  auto ServiceId = Builder.CreateString(StaticStatusInformation.ServiceID);
  auto HostName = Builder.CreateString(StaticStatusInformation.HostName);
  uint32_t ProcessId = StaticStatusInformation.ProcessID;
  uint32_t UpdateInterval = toMilliSeconds(Period);
  auto JSONStatus = Builder.CreateString(JSONReport);

  auto MsgBuffer = FlatBuffer::CreateStatus(
      Builder, SoftwareName, SoftwareVersion, ServiceId, HostName, ProcessId,
      UpdateInterval, JSONStatus);
  FlatBuffer::FinishStatusBuffer(Builder, MsgBuffer);
  return Builder.Release();
}

// Create the JSON part of the status message
std::string StatusReporterBase::createJSONReport() const {
  auto Info = nlohmann::json::object();
  std::lock_guard<std::mutex> const lock(StatusMutex);
  std::map<JobStatusInfo::WorkerState, std::string> StateMap{
      {JobStatusInfo::WorkerState::Idle, "idle"},
      {JobStatusInfo::WorkerState::Writing, "writing"}};
  Info["state"] = StateMap[Status.State];
  Info["job_id"] = Status.JobId;
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
