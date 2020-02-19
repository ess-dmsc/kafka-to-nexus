//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file This file defines the different success and failure status that the
/// `StreamMaster` and the `Streamer` can incur. These error object have some
/// utility methods that can be used to test the more common situations.

#include <6s4t_run_stop_generated.h>
#include <pl72_run_start_generated.h>

#include "CommandParser.h"
#include "Msg.h"

namespace FileWriter {
namespace CommandParser {

StartCommandInfo
extractStartInformation(Msg const &CommandMessage,
                        std::chrono::milliseconds DefaultStartTime) {
  StartCommandInfo Result;

  const auto runStartData = GetRunStart(CommandMessage.data());

  if (runStartData->start_time() > 0) {
    Result.StartTime = std::chrono::milliseconds{runStartData->start_time()};
  } else {
    Result.StartTime = DefaultStartTime;
  }
  Result.StopTime = std::chrono::milliseconds{runStartData->stop_time()};
  Result.NexusStructure = runStartData->nexus_structure()->str();
  Result.JobID = runStartData->job_id()->str();
  Result.ServiceID = runStartData->service_id()->str();
  Result.BrokerInfo = uri::URI(runStartData->broker()->str());
  Result.Filename = runStartData->filename()->str();

  // TODO JobID, NexusStructure, Filename, Broker are required
  //   log error if any are missing
  //   log if broker URI is malformed
  //   any other verification?

  return Result;
}

StopCommandInfo extractStopInformation(Msg const &CommandMessage) {
  StopCommandInfo Result;

  const auto runStopData = GetRunStop(CommandMessage.data());
  Result.JobID = runStopData->job_id()->str();
  Result.StopTime = std::chrono::milliseconds{runStopData->stop_time()};
  Result.ServiceID = runStopData->service_id()->str();

  // TODO JobID required, log error if missing

  return Result;
}

bool isStartCommand(Msg const &CommandMessage) {
  if (!flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                        RunStartIdentifier())) {
    return false;
  }

  auto BufferVerifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyRunStartBuffer(BufferVerifier);
}

bool isStopCommand(Msg const &CommandMessage) {
  if (!flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                        RunStopIdentifier())) {
    return false;
  }

  auto BufferVerifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  return VerifyRunStopBuffer(BufferVerifier);
}

} // namespace CommandParser
} // namespace FileWriter
