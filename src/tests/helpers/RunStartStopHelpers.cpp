#include "Msg.h"

#include <6s4t_run_stop_generated.h>
#include <pl72_run_start_generated.h>

namespace RunStartStopHelpers {
FileWriter::Msg buildRunStartMessage(
    std::string const &InstrumentName, std::string const &RunName,
    std::string const &NexusStructure, std::string const &JobID,
    std::string const &ServiceID, std::string const &Broker,
    std::string const &Filename, uint64_t StartTime, uint64_t StopTime) {
  flatbuffers::FlatBufferBuilder Builder;

  const auto InstrumentNameOffset = Builder.CreateString(InstrumentName);
  const auto RunIDOffset = Builder.CreateString(RunName);
  const auto NexusStructureOffset = Builder.CreateString(NexusStructure);
  const auto JobIDOffset = Builder.CreateString(JobID);
  const auto ServiceIDOffset = Builder.CreateString(ServiceID);
  const auto BrokerOffset = Builder.CreateString(Broker);
  const auto FilenameOffset = Builder.CreateString(Filename);

  auto messageRunStart =
      CreateRunStart(Builder, StartTime, StopTime, RunIDOffset,
                     InstrumentNameOffset, NexusStructureOffset, JobIDOffset,
                     BrokerOffset, ServiceIDOffset, FilenameOffset);

  FinishRunStartBuffer(Builder, messageRunStart);
  auto MessageBuffer = Builder.Release();
  return {MessageBuffer.data(), MessageBuffer.size()};
}

FileWriter::Msg buildRunStopMessage(uint64_t StopTime,
                                    std::string const &RunName,
                                    std::string const &JobID,
                                    std::string const &ServiceID) {
  flatbuffers::FlatBufferBuilder Builder;

  const auto RunIDOffset = Builder.CreateString(RunName);
  const auto JobIDOffset = Builder.CreateString(JobID);
  const auto ServiceIDOffset = Builder.CreateString(ServiceID);

  auto messageRunStop = CreateRunStop(Builder, StopTime, RunIDOffset,
                                      JobIDOffset, ServiceIDOffset);

  FinishRunStopBuffer(Builder, messageRunStop);
  auto MessageBuffer = Builder.Release();
  return {MessageBuffer.data(), MessageBuffer.size()};
}
} // namespace RunStartStopHelpers
