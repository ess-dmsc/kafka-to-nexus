#pragma once

#include "logger.h"
#include <string>
#include <vector>

// Forward declarations
namespace CLI {
class App;
class Option;
}
namespace uri {
struct URI;
}
struct MainOpt;

void setCLIOptions(CLI::App &App, MainOpt &MainOptions);

CLI::Option *addUriOption(CLI::App &App, std::string const &Name,
                          uri::URI &URIArg, bool &TrueIfOptionGiven,
                          std::string const &Description, bool Defaulted);

/// Use for adding a URI option
CLI::Option *addOption(CLI::App &App, std::string const &Name, uri::URI &URIArg,
                       std::string const &Description, bool Defaulted);

/// \brief Parsing log level from user's input.
/// Look for \p LogLevelString value in a map containing spdlog levels.
/// \param LogLevelString User's input
/// \param LogLevelResult Result of parsing returned through reference
/// \return bool signalizing successful parsing
bool parseLogLevel(std::vector<std::string> LogLevelString,
                   spdlog::level::level_enum &LogLevelResult);

