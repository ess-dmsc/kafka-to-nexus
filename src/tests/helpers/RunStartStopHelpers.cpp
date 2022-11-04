#include "RunStartStopHelpers.h"
#include "Msg.h"

#include <6s4t_run_stop_generated.h>
#include <optional>
#include <pl72_run_start_generated.h>

namespace RunStartStopHelpers {
FileWriter::Msg buildRunStartMessage(
    std::string const &InstrumentName, std::string const &RunName,
    std::string const &NexusStructure, std::string const &JobID,
    std::optional<std::string> const &ServiceID, std::string const &Filename,
    uint64_t StartTime, uint64_t StopTime) {
  flatbuffers::FlatBufferBuilder Builder;

  const auto InstrumentNameOffset = Builder.CreateString(InstrumentName);
  const auto RunIDOffset = Builder.CreateString(RunName);
  const auto NexusStructureOffset = Builder.CreateString(NexusStructure);
  const auto JobIDOffset = Builder.CreateString(JobID);
  flatbuffers::Offset<flatbuffers::String> ServiceIDOffset;
  if (ServiceID) {
    ServiceIDOffset = Builder.CreateString(*ServiceID);
  }
  const auto FilenameOffset = Builder.CreateString(Filename);

  RunStartBuilder StartBuilder{Builder};
  StartBuilder.add_start_time(StartTime);
  StartBuilder.add_stop_time(StopTime);
  StartBuilder.add_run_name(RunIDOffset);
  StartBuilder.add_instrument_name(InstrumentNameOffset);
  StartBuilder.add_nexus_structure(NexusStructureOffset);
  StartBuilder.add_job_id(JobIDOffset);
  if (ServiceID) {
    StartBuilder.add_service_id(ServiceIDOffset);
  }
  StartBuilder.add_filename(FilenameOffset);
  auto messageRunStart = StartBuilder.Finish();

  FinishRunStartBuffer(Builder, messageRunStart);
  auto MessageBuffer = Builder.Release();
  return {MessageBuffer.data(), MessageBuffer.size()};
}

FileWriter::Msg
buildRunStopMessage(uint64_t StopTime, std::string const &RunName,
                    std::string const &JobID, std::string const &CommandID,
                    std::optional<std::string> const &ServiceID) {
  flatbuffers::FlatBufferBuilder Builder;

  const auto RunIDOffset = Builder.CreateString(RunName);
  const auto JobIDOffset = Builder.CreateString(JobID);
  const auto CommandIDOffset = Builder.CreateString(CommandID);
  flatbuffers::Offset<flatbuffers::String> ServiceIDOffset;
  if (ServiceID) {
    ServiceIDOffset = Builder.CreateString(*ServiceID);
  }

  RunStopBuilder StopBuilder{Builder};
  StopBuilder.add_stop_time(StopTime);
  StopBuilder.add_run_name(RunIDOffset);
  StopBuilder.add_job_id(JobIDOffset);
  StopBuilder.add_command_id(CommandIDOffset);
  if (ServiceID) {
    StopBuilder.add_service_id(ServiceIDOffset);
  }
  auto messageRunStop = StopBuilder.Finish();

  FinishRunStopBuffer(Builder, messageRunStop);
  auto MessageBuffer = Builder.Release();
  return {MessageBuffer.data(), MessageBuffer.size()};
}
} // namespace RunStartStopHelpers
