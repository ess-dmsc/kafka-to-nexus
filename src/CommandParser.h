// SPDX-License-Identifier: BSD-2-Clause
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

#pragma once

#include "URI.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <chrono>

namespace FileWriter {

struct StartCommandInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string ServiceID;
  bool UseSwmr;
  bool AbortOnStreamFailure;
  uri::URI BrokerInfo{"localhost:9092"};
  std::chrono::milliseconds StartTime{0};
  std::chrono::milliseconds StopTime{0};
};

struct StopCommandInfo {
  std::string JobID;
  std::chrono::milliseconds StopTime{0};
  std::string ServiceID;
};

namespace CommandParser {
static std::string const StartCommand = "filewriter_new";
static std::string const StopCommand = "filewriter_stop";
static std::string const ExitCommand = "filewriter_exit";
static std::string const StopAllWritingCommand = "file_writer_tasks_clear_all";

/// \brief Extract the information from the start command.
///
/// \param JSONCommand The JSON Command.
/// \param DefaultStartTime The start time to use if not supplied in the JSON
/// \return The start information.
StartCommandInfo extractStartInformation(
    const nlohmann::json &JSONCommand,
    std::chrono::milliseconds DefaultStartTime = getCurrentTimeStampMS());

/// \brief Extract the information from the stop command.
///
/// \param JSONCommand The JSON Command.
/// \return The stop information.
StopCommandInfo extractStopInformation(const nlohmann::json &JSONCommand);

/// \brief Extract the command name from the command.
///
/// Note: the command is converted to lower-case.
///
/// \param JSONCommand The JSON Command.
/// \return The command name.
std::string extractCommandName(const nlohmann::json &JSONCommand);

/// \brief Extract the broker from the command.
///
/// \param JSONCommand The JSON Command.
/// \return The broker details.
uri::URI extractBroker(nlohmann::json const &JSONCommand);

/// \brief Extract the job ID from the command.
///
/// \param JSONCommand The JSON Command.
/// \return The job ID.
std::string extractJobID(nlohmann::json const &JSONCommand);

/// \brief Extract a value as a time-stamp.
///
/// \param Key The key for the value to extract.
/// \param JSONCommand The JSON Command.
/// \param DefaultTime The time to use if the key does not exist.
/// \return The extract (or default) time.
std::chrono::milliseconds
extractTime(std::string const &Key, nlohmann::json const &JSONCommand,
            std::chrono::milliseconds const &DefaultTime);

/// \brief Extract the specified value.
///
/// Throws if the key is not present
///
/// \param Key The key for the value to extract.
/// \param JSONCommand The JSON Command.
/// \return The extracted value.
template <typename T>
T getRequiredValue(std::string const &Key, nlohmann::json const &JSONCommand) {
  if (auto x = find<T>(Key, JSONCommand)) {
    return x.inner();
  }

  throw std::runtime_error(
      fmt::format("Missing key {} from command JSON", Key));
}

/// \brief Extract the specified value or use supplied value.
///
///
/// \param Key The key for the value to extract.
/// \param JSONCommand The JSON Command.
/// \param Default The value to use if the key is not present.
/// \return The extracted (or default) value.
template <typename T>
T getOptionalValue(std::string const &Key, nlohmann::json const &JSONCommand,
                   T const &Default) {
  if (auto x = find<T>(Key, JSONCommand)) {
    return x.inner();
  }

  return Default;
}
}
} // namespace FileWriter
