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
} // namespace FlatBuffer

namespace Status {

time_point StatusReporterBase::getStopTime() const {
  std::shared_lock const lock(StatusMutex);
  return StatusGetter().StopTime;
}

flatbuffers::DetachedBuffer
StatusReporterBase::createReport(std::string const &JSONReport) const {
  std::shared_lock const lock(StatusMutex);

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
nlohmann::json StatusReporterBase::createJSONReport() const {
  std::shared_lock const lock(StatusMutex);
  auto Info = nlohmann::json::object();
  auto CurrentStatus = StatusGetter();
  std::map<Status::WorkerState, std::string> StateMap{
      {Status::WorkerState::Idle, "idle"},
      {Status::WorkerState::Writing, "writing"}};
  Info["state"] = StateMap[CurrentStatus.State];
  Info["job_id"] = CurrentStatus.JobId;
  Info["file_being_written"] = CurrentStatus.Filename;
  Info["start_time"] = toMilliSeconds(CurrentStatus.StartTime);
  Info["stop_time"] = toMilliSeconds(CurrentStatus.StopTime);
  return Info;
}

void StatusReporterBase::reportStatus() {
  std::shared_lock const Lock{StatusMutex};
  if (!StatusProducerTopic) {
    return;
  }
  try {
    auto const StatusJSONReport = createJSONReport();
    auto const StatusReportString = StatusJSONReport.dump();
    if (StatusJSONReport["file_being_written"] != "") {
      LOG_DEBUG("status: {}", StatusReportString);
    }

    StatusProducerTopic->produce(createReport(StatusReportString));
    postReportStatusActions();
  } catch (std::runtime_error &E) {
    LOG_WARN("Unable to create a status report. The error was: {}", E.what());
  }
}

void StatusReporterBase::useAlternativeStatusTopic(
    std::string const &AltTopicName) {

  if (not UsingAlternativeStatusTopic) {
    AltStatusProducerTopic =
        std::make_unique<Kafka::ProducerTopic>(Producer, AltTopicName);
    std::swap(StatusProducerTopic, AltStatusProducerTopic);
    UsingAlternativeStatusTopic = true;
    LOG_DEBUG(R"(Now using the alternative status topic "{}".)", AltTopicName);
  } else {
    LOG_WARN(R"(Unable to set new alternative status topic "{}" as the "
             "alternative topic "{}" is already used.)",
             AltTopicName, StatusProducerTopic->name());
  }
}

void StatusReporterBase::revertToDefaultStatusTopic() {
  if (UsingAlternativeStatusTopic) {
    std::swap(StatusProducerTopic, AltStatusProducerTopic);
    UsingAlternativeStatusTopic = false;
    LOG_DEBUG(R"(Reverting to default status topic name "{}".)",
              StatusProducerTopic->name());
  }
}

} // namespace Status
