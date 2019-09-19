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

std::string const CommandParser::StopCommand = "filewriter_stop";
std::string const CommandParser::StartCommand = "filewriter_new";
std::string const CommandParser::ExitCommand = "filewriter_exit";
std::string const CommandParser::StopAllWritingCommand = "file_writer_tasks_clear_all";

StartCommandInfo CommandParser::extractStartInformation(const nlohmann::json &JSONCommand,
                                            std::chrono::milliseconds DefaultStartTime) {
  if (extractCommandName(JSONCommand) != StartCommand) {
    throw std::runtime_error("Command was not a start command");
  }

  StartCommandInfo Result;

  // Required items
  Result.JobID = extractJobID(JSONCommand);
  Result.NexusStructure = getRequiredValue<nlohmann::json>("nexus_structure", JSONCommand).dump();
  auto FileAttributes = getRequiredValue<nlohmann::json>("file_attributes", JSONCommand);
  Result.Filename = getRequiredValue<std::string>("file_name", FileAttributes);

  // Optional items
  Result.BrokerInfo = extractBroker(JSONCommand);
  Result.StartTime = extractTime("start_time", JSONCommand, DefaultStartTime);
  Result.StopTime = extractTime("stop_time", JSONCommand, std::chrono::milliseconds::zero());
  Result.UseSwmr = getOptionalValue<bool>("use_hdf_swmr", JSONCommand, true);
  Result.AbortOnStreamFailure = getOptionalValue<bool>("abort_on_uninitialised_stream", JSONCommand, false);
  Result.ServiceID = getOptionalValue<std::string>("service_id", JSONCommand, "");

  return Result;
}

StopCommandInfo CommandParser::extractStopInformation(const nlohmann::json &JSONCommand) {
  if (extractCommandName(JSONCommand) != StopCommand) {
    throw std::runtime_error("Command was not a stop command");
  }

  StopCommandInfo Result;

  // Required items
  Result.JobID = extractJobID(JSONCommand);

  // Optional items
  Result.StopTime = extractTime("stop_time", JSONCommand, std::chrono::milliseconds::zero());
  Result.ServiceID = getOptionalValue<std::string>("service_id", JSONCommand, "");

  return Result;
}

uri::URI CommandParser::extractBroker(nlohmann::json const &JSONCommand) {
  // Set to default
  uri::URI BrokerInfo{"localhost:9092"};

  std::string Broker = getOptionalValue<std::string>("broker", JSONCommand, BrokerInfo.HostPort);
  if (!Broker.empty()) {
    try {
      BrokerInfo.parse(Broker);
    } catch (std::runtime_error &e) {
      Logger->warn("Unable to parse broker {} in command message, using "
                   "default broker",
                   Broker);
    }
  }

  return BrokerInfo;
}

std::string CommandParser::extractJobID(nlohmann::json const &JSONCommand) {
  auto JobID = getRequiredValue<std::string>("job_id", JSONCommand);
  if (JobID.empty()) {
    throw std::runtime_error("Missing key job_id from command JSON");
  }
  return JobID;
}

std::chrono::duration<long long int, std::milli> CommandParser::getCurrentTime() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
}

std::chrono::milliseconds CommandParser::extractTime(std::string const & Key, nlohmann::json const &JSONCommand, std::chrono::milliseconds const &DefaultTime) {
  uint64_t RawTime = getOptionalValue(Key, JSONCommand, 0);
  if (RawTime > 0) {
    return std::chrono::milliseconds{RawTime};
  }
  else {
    return DefaultTime;
  }
}

std::string CommandParser::extractCommandName(const nlohmann::json &JSONCommand) {
  auto Cmd = getRequiredValue<std::string>("cmd", JSONCommand);
  std::transform(Cmd.begin(), Cmd.end(), Cmd.begin(),
                 ::tolower);
  return Cmd;
}

}

