// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "logger.h"
#include <string>

// Forward declarations
namespace CLI {
class App;
class Option;
} // namespace CLI
namespace uri {
struct URI;
}
struct MainOpt;

void setCLIOptions(CLI::App &App, MainOpt &MainOptions);

CLI::Option *addUriOption(CLI::App &App, std::string const &Name,
                          uri::URI &URIArg, bool &TrueIfOptionGiven,
                          std::string const &Description, bool Defaulted);

/// Use for adding a URI option
CLI::Option *addUriOption(CLI::App &App, std::string const &Name,
                          uri::URI &URIArg, std::string const &Description,
                          bool Defaulted);

/// \brief Parsing log level from user's input.
/// Look for \p LogLevelString value in a map containing spdlog levels.
/// \param LogLevelString User's input
/// \param LogLevelResult Result of parsing returned through reference
/// \return bool signalizing successful parsing
bool parseLogLevel(std::vector<std::string> LogLevelString,
                   Log::Severity &LogLevelResult);
