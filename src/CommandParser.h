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

#include <chrono>

#include "URI.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {

struct StartCommandInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string ServiceID;
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
/// \brief Extract the information from the start command.
///
/// \param JSONCommand The JSON Command.
/// \param DefaultStartTime The start time to use if not supplied in the JSON
/// \return The start information.
StartCommandInfo extractStartInformation(
    Msg const &JSONCommand,
    std::chrono::milliseconds DefaultStartTime = getCurrentTimeStampMS());

/// \brief Extract the information from the stop command.
///
/// \param JSONCommand The JSON Command.
/// \return The stop information.
StopCommandInfo extractStopInformation(Msg const &JSONCommand);
} // namespace CommandParser
} // namespace FileWriter
