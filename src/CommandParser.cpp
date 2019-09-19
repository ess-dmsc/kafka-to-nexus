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

StartCommandInfo CommandParser::extractStartInformation(const nlohmann::json &JSONCommand,
                                            std::chrono::milliseconds DefaultStartTime) {
  StartCommandInfo Result;
  
  // Required items
  extractJobID(JSONCommand, Result.JobID);
  Result.NexusStructure = getRequiredValue<nlohmann::json>("nexus_structure", JSONCommand).dump();
  auto FileAttributes = getRequiredValue<nlohmann::json>("file_attributes", JSONCommand);
  Result.Filename = getRequiredValue<std::string>("file_name", FileAttributes);

  // Optional items
  extractBroker(JSONCommand, Result.BrokerInfo);
  setOptionalValue<bool>("use_hdf_swmr", JSONCommand, Result.UseSwmr);
  setOptionalValue<bool>("abort_on_uninitialised_stream", JSONCommand, Result.AbortOnStreamFailure);
  Result.StartTime = extractTime("start_time", JSONCommand, DefaultStartTime);
  Result.StopTime = extractTime("stop_time", JSONCommand, std::chrono::milliseconds::zero());
  setOptionalValue<std::string>("service_id", JSONCommand, Result.ServiceID);

  return Result;
}

void CommandParser::extractBroker(nlohmann::json const &JSONCommand, uri::URI &BrokerInfo) {
  std::string Broker;
  setOptionalValue("broker", JSONCommand, Broker);
  if (!Broker.empty()) {
    try {
      BrokerInfo.parse(Broker);
    } catch (std::runtime_error &e) {
      Logger->warn("Unable to parse broker {} in command message, using "
                   "default broker",
                   Broker);
    }
  }
}

void CommandParser::extractJobID(nlohmann::json const &JSONCommand, std::string & JobID) {
  JobID = getRequiredValue<std::string>("job_id", JSONCommand);
  if (JobID.empty()) {
    throw std::runtime_error("Missing key job_id from command JSON");
  }
}

std::chrono::duration<long long int, std::milli> CommandParser::getCurrentTime() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
}

std::chrono::milliseconds CommandParser::extractTime(std::string const & Key, nlohmann::json const &JSONCommand, std::chrono::milliseconds const &DefaultTime) {
  uint64_t RawTime = 0;
  setOptionalValue(Key, JSONCommand, RawTime);
  if (RawTime > 0) {
    return std::chrono::milliseconds{RawTime};
  }
  else {
    return DefaultTime;
  }
}

}

