#include "StatusHelpers.h"
#include "Kafka/ProducerMessage.h"
#include "Status/StatusInfo.h"
#include "json.h"

#include <flatbuffers/flatbuffers.h>

namespace FlatBuffer {
#include <x5f2_status_generated.h>
}

using nlohmann::json;

std::pair<Status::JobStatusInfo, Status::ApplicationStatusInfo>
deserialiseStatusMessage(flatbuffers::DetachedBuffer const &Message) {
  const auto statusData = FlatBuffer::GetStatus(Message.data());
  std::string const SoftwareName =
      flatbuffers::GetString(statusData->software_name());
  std::string const SoftwareVersion =
      flatbuffers::GetString(statusData->software_version());
  std::string const ServiceId =
      flatbuffers::GetString(statusData->service_id());
  std::string const HostName = flatbuffers::GetString(statusData->host_name());
  int32_t const ProcessId = statusData->process_id();
  auto const UpdateInterval =
      std::chrono::milliseconds{statusData->update_interval()};
  std::string const StatusJSONString =
      flatbuffers::GetString(statusData->status_json());

  auto const StatusJSON = json::parse(StatusJSONString);
  auto const JobId = find<std::string>("job_id", StatusJSON);
  auto const Filename = find<std::string>("file_being_written", StatusJSON);
  auto const StartTime = find<uint64_t>("start_time", StatusJSON);
  auto const StopTime = find<uint64_t>("stop_time", StatusJSON);

  return {
      Status::JobStatusInfo{
          Status::WorkerState::Idle, JobId.value_or(""), Filename.value_or(""),
          time_point{std::chrono::milliseconds{StartTime.value_or(0)}},
          time_point{std::chrono::milliseconds{StopTime.value_or(0)}}},
      Status::ApplicationStatusInfo{UpdateInterval, SoftwareName,
                                    SoftwareVersion, HostName,
                                    "No service name", ServiceId, ProcessId}};
}
