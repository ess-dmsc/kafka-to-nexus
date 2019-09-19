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

class CommandParser {
public:
  /// \brief Extract the information from the start command.
  ///
  /// \param JSONCommand The JSON Command.
  /// \param DefaultStartTime The start time to use if not supplied in the JSON
  /// \return The start information.
  StartCommandInfo extractStartInformation(
      const nlohmann::json &JSONCommand,
      std::chrono::milliseconds DefaultStartTime = getCurrentTime());

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
  static std::string extractCommandName(const nlohmann::json &JSONCommand);

  static std::string const StopCommand;
  static std::string const StartCommand;
  static std::string const ExitCommand;
  static std::string const StopAllWritingCommand;

private:
  SharedLogger Logger = getLogger();

  uri::URI extractBroker(nlohmann::json const &JSONCommand);
  static std::string extractJobID(nlohmann::json const &JSONCommand);
  static std::chrono::duration<long long int, std::milli> getCurrentTime();
  static std::chrono::milliseconds
  extractTime(std::string const &Key, nlohmann::json const &JSONCommand,
              std::chrono::milliseconds const &DefaultTime);

  template <typename T>
  static T getRequiredValue(std::string const &Key,
                            nlohmann::json const &JSONCommand) {
    if (auto x = find<T>(Key, JSONCommand)) {
      return x.inner();
    }

    throw std::runtime_error(
        fmt::format("Missing key {} from command JSON", Key));
  }

  template <typename T>
  static T getOptionalValue(std::string const &Key,
                            nlohmann::json const &JSONCommand,
                            T const &Default) {
    if (auto x = find<T>(Key, JSONCommand)) {
      return x.inner();
    }

    return Default;
  }
};
} // namespace FileWriter
