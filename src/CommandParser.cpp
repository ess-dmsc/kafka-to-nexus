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

#include "CommandParser.h"

namespace FileWriter {
namespace CommandParser {

StartCommandInfo
extractStartInformation(const nlohmann::json &JSONCommand,
                        std::chrono::milliseconds DefaultStartTime) {
  if (extractCommandName(JSONCommand) != StartCommand) {
    throw std::runtime_error("Command was not a start command");
  }

  StartCommandInfo Result;

  // Required items
  Result.JobID = extractJobID(JSONCommand);
  Result.NexusStructure =
      getRequiredValue<nlohmann::json>("nexus_structure", JSONCommand).dump();
  auto FileAttributes =
      getRequiredValue<nlohmann::json>("file_attributes", JSONCommand);
  Result.Filename = getRequiredValue<std::string>("file_name", FileAttributes);
  Result.BrokerInfo = extractBroker(JSONCommand);

  // Optional items
  Result.StartTime = extractTime("start_time", JSONCommand, DefaultStartTime);
  Result.StopTime =
      extractTime("stop_time", JSONCommand, std::chrono::milliseconds::zero());
  Result.UseSwmr = getOptionalValue<bool>("use_hdf_swmr", JSONCommand, true);
  Result.AbortOnStreamFailure = getOptionalValue<bool>(
      "abort_on_uninitialised_stream", JSONCommand, false);
  Result.ServiceID =
      getOptionalValue<std::string>("service_id", JSONCommand, "");

  return Result;
}

StopCommandInfo extractStopInformation(const nlohmann::json &JSONCommand) {
  if (extractCommandName(JSONCommand) != StopCommand) {
    throw std::runtime_error("Command was not a stop command");
  }

  StopCommandInfo Result;

  // Required items
  Result.JobID = extractJobID(JSONCommand);

  // Optional items
  Result.StopTime =
      extractTime("stop_time", JSONCommand, std::chrono::milliseconds::zero());
  Result.ServiceID =
      getOptionalValue<std::string>("service_id", JSONCommand, "");

  return Result;
}

uri::URI extractBroker(nlohmann::json const &JSONCommand) {
  std::string Broker = getRequiredValue<std::string>("broker", JSONCommand);
  try {
    uri::URI BrokerInfo{Broker};
    return BrokerInfo;
  } catch (std::runtime_error &e) {
    throw std::runtime_error(
        fmt::format("Unable to parse broker {} in command message", Broker));
  }
}

std::string extractJobID(nlohmann::json const &JSONCommand) {
  auto JobID = getRequiredValue<std::string>("job_id", JSONCommand);
  if (JobID.empty()) {
    throw std::runtime_error("Missing key job_id from command JSON");
  }
  return JobID;
}

std::chrono::milliseconds
extractTime(std::string const &Key, nlohmann::json const &JSONCommand,
            std::chrono::milliseconds const &DefaultTime) {
  auto RawTime = getOptionalValue<uint64_t>(Key, JSONCommand, 0);
  if (RawTime > 0) {
    return std::chrono::milliseconds{RawTime};
  } else {
    return DefaultTime;
  }
}

std::string extractCommandName(const nlohmann::json &JSONCommand) {
  auto Cmd = getRequiredValue<std::string>("cmd", JSONCommand);
  std::transform(Cmd.begin(), Cmd.end(), Cmd.begin(), ::tolower);
  return Cmd;
}
} // namespace CommandParser
} // namespace FileWriter
