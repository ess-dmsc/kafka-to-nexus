// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "TimeUtility.h"

#include "URI.h"
#include "helper.h"
#include "json.h"
#include "logger.h"

namespace FileWriter {

struct Msg;

struct StartCommandInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string ServiceID;
  uri::URI BrokerInfo{"localhost:9092"};
  std::chrono::milliseconds StartTime{0};
  time_point StopTime{time_point::max()};
};

struct StopCommandInfo {
  std::string JobID;
  std::chrono::milliseconds StopTime{0};
  std::string ServiceID;
};

namespace CommandParser {
/// \brief Extract the information from the start command.
///
/// \param JSONCommand The JSON Command.
/// \param DefaultStartTime The start time to use if not supplied in the JSON
/// \return The start information.
StartCommandInfo extractStartInformation(
    Msg const &CommandMessage,
    std::chrono::milliseconds DefaultStartTime = getCurrentTimeStampMS());

/// \brief Extract the information from the stop command.
///
/// \param JSONCommand The JSON Command.
/// \return The stop information.
StopCommandInfo extractStopInformation(Msg const &CommandMessage);

bool isStartCommand(Msg const &CommandMessage);
bool isStopCommand(Msg const &CommandMessage);

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
    return *x;
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
    return *x;
  }

  return Default;
}
} // namespace CommandParser
} // namespace FileWriter
