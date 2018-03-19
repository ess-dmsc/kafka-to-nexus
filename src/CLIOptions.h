#pragma once

#include <string>

// Forward declarations
namespace CLI {
class App;
class Option;
}
namespace uri {
class URI;
}
struct MainOpt;

void setCLIOptions(CLI::App &App, MainOpt &MainOptions);

CLI::Option *add_option(CLI::App &App, std::string Name, uri::URI &URIArg,
                        bool &TrueIfOptionGiven, std::string Description,
                        bool Defaulted);

CLI::Option *add_option(CLI::App &App, std::string Name, uri::URI &URIArg,
                        std::string Description, bool Defaulted);
