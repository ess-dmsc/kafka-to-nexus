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
#include <sstream>

#include "CommandParser.h"
#include "Msg.h"

namespace {
void checkRequiredFieldsArePresent(const RunStart *RunStartData) {
  std::stringstream Errors;
  if (RunStartData->job_id()->size() == 0) {
    Errors << "Job ID missing, this field is required\n";
  }

  if (RunStartData->nexus_structure()->size() == 0) {
    Errors << "NeXus Structure missing, this field is "
              "required\n";
  }

  if (RunStartData->filename()->size() == 0) {
    Errors << "Filename missing, this field is required\n";
  }

  if (RunStartData->broker()->size() == 0) {
    Errors << "Broker missing, this field is required\n";
  } else {
    try {
      uri::URI(RunStartData->broker()->str());
    } catch (const std::runtime_error &URIError) {
      Errors << "Broker missing, this field is required\n";
    }
  }

  std::string const ErrorsString = Errors.str();
  if (!ErrorsString.empty()) {
    throw std::runtime_error(fmt::format(
        "Errors encountered parsing run start message:\n{}", ErrorsString));
  }
}
} // namespace

namespace FileWriter {
namespace CommandParser {

StartCommandInfo
extractStartInformation(Msg const &CommandMessage,
                        std::chrono::milliseconds DefaultStartTime) {
  StartCommandInfo Result;

  const auto RunStartData = GetRunStart(CommandMessage.data());

  checkRequiredFieldsArePresent(RunStartData);

  if (RunStartData->start_time() > 0) {
    Result.StartTime = std::chrono::milliseconds{RunStartData->start_time()};
  } else {
    Result.StartTime = DefaultStartTime;
  }
  Result.StopTime = std::chrono::milliseconds{RunStartData->stop_time()};
  Result.NexusStructure = RunStartData->nexus_structure()->str();
  Result.JobID = RunStartData->job_id()->str();
  Result.ServiceID = RunStartData->service_id()->str();
  Result.BrokerInfo = uri::URI(RunStartData->broker()->str());
  Result.Filename = RunStartData->filename()->str();

  return Result;
}

StopCommandInfo extractStopInformation(Msg const &CommandMessage) {
  StopCommandInfo Result;

  const auto RunStopData = GetRunStop(CommandMessage.data());

  if (RunStopData->job_id()->size() == 0) {
    throw std::runtime_error("Errors encountered parsing run stop message:\n"
                             "Job ID missing, this field is required");
  }

  Result.JobID = RunStopData->job_id()->str();
  Result.StopTime = std::chrono::milliseconds{RunStopData->stop_time()};
  Result.ServiceID = RunStopData->service_id()->str();

  return Result;
}

bool isStartCommand(Msg const &CommandMessage) {
  if (!flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                        RunStartIdentifier())) {
    return false;
  }

  auto BufferVerifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  auto VerifiedStartCommand = VerifyRunStartBuffer(BufferVerifier);
  if (!VerifiedStartCommand) {
    auto Logger = getLogger();
    Logger->warn("Message with start command identifer received, but it failed "
                 "flatbuffer verification");
  }
  return VerifiedStartCommand;
}

bool isStopCommand(Msg const &CommandMessage) {
  if (!flatbuffers::BufferHasIdentifier(CommandMessage.data(),
                                        RunStopIdentifier())) {
    return false;
  }

  auto BufferVerifier =
      flatbuffers::Verifier(CommandMessage.data(), CommandMessage.size());
  auto VerifiedStopCommand = VerifyRunStopBuffer(BufferVerifier);
  if (!VerifiedStopCommand) {
    auto Logger = getLogger();
    Logger->warn("Message with stop command identifer received, but it failed "
                 "flatbuffer verification");
  }
  return VerifiedStopCommand;
}

} // namespace CommandParser
} // namespace FileWriter
