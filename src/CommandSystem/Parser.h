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

#include "Commands.h"
#include "Msg.h"
#include "URI.h"
#include "helper.h"
#include "json.h"
#include "logger.h"

using FileWriter::Msg;

namespace Command {

namespace Parser {
/// \brief Extract the information from the start command.
///
/// \param JSONCommand The JSON Command.
/// \param DefaultStartTime The start time to use if not supplied in the JSON
/// \return The start information.
StartMessage
extractStartMessage(Msg const &CommandMessage,
                    time_point DefaultStartTime = system_clock::now());

/// \brief Extract the information from the stop command.
///
/// \param JSONCommand The JSON Command.
/// \return The stop information.
StopMessage extractStopMessage(Msg const &CommandMessage);

/// \brief Is the provided command a start command?
bool isStartCommand(Msg const &CommandMessage);

/// \brief Is the provided command a stop command?
bool isStopCommand(Msg const &CommandMessage);

/// \brief Is the provided message a status message?
bool isStatusMessage(Msg const &CommandMessage);

/// \brief Is the provided message an answer?
bool isAnswerMessage(Msg const &CommandMessage);

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
} // namespace Parser
} // namespace Command
