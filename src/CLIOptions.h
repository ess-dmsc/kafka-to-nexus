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

CLI::Option *addOption(CLI::App &App, std::string const &Name, uri::URI &URIArg,
                       bool &TrueIfOptionGiven, std::string const &Description,
                       bool Defaulted);

/// Use for adding a URI option
CLI::Option *addOption(CLI::App &App, std::string const &Name, uri::URI &URIArg,
                       std::string const &Description, bool Defaulted);

bool parseLogLevel(std::vector<std::string> LogLevelString,
                   spdlog::level::level_enum &LogLevelResult);