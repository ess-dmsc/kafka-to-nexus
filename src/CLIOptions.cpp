// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CLIOptions.h"
#include "MainOpt.h"
#include "URI.h"
#include <CLI/CLI.hpp>

CLI::Option *uriOption(CLI::App &App, const std::string &Name,
                       CLI::callback_t Fun, const std::string &Description,
                       bool Defaulted) {

  CLI::Option *Opt =
      App.add_option(Name, std::move(Fun), Description, Defaulted);
  Opt->type_name("URI");
  Opt->type_size(1);
  return Opt;
}

CLI::Option *addUriOption(CLI::App &App, std::string const &Name,
                          uri::URI &URIArg, std::string const &Description = "",
                          bool Defaulted = false) {
  CLI::callback_t Fun = [&URIArg](CLI::results_t Results) {
    try {
      URIArg.parse(Results[0]);
    } catch (std::runtime_error &E) {
      return false;
    }
    return true;
  };

  return uriOption(App, Name, Fun, Description, Defaulted);
}

/// \brief Adding a URI option.
///
/// If the URI is given then TrueIfOptionGiven is set to true
///
/// \param App
/// \param Name
/// \param URIArg
/// \param TrueIfOptionGiven
/// \param Description
/// \param Defaulted
/// \return
CLI::Option *addUriOption(CLI::App &App, const std::string &Name,
                          uri::URI &URIArg, bool &TrueIfOptionGiven,
                          const std::string &Description = "",
                          bool Defaulted = false) {
  CLI::callback_t Fun = [&URIArg, &TrueIfOptionGiven](CLI::results_t Results) {
    TrueIfOptionGiven = true;
    try {
      URIArg.parse(Results[0]);
    } catch (std::runtime_error &E) {
      return false;
    }
    return true;
  };

  return uriOption(App, Name, Fun, Description, Defaulted);
}

void addMillisecondOption(CLI::App &App, const std::string &Name,
                          std::chrono::milliseconds &MSArg,
                          const std::string &Description = "",
                          bool Defaulted = false) {
  CLI::callback_t Fun = [&MSArg](CLI::results_t Results) {
    MSArg = std::chrono::milliseconds(std::stoi(Results[0]));
    return true;
  };
  App.add_option(Name, Fun, Description, Defaulted);
}

CLI::Option *SetKeyValueOptions(CLI::App &App, const std::string &Name,
                                const std::string &Description, bool Defaulted,
                                const CLI::callback_t &Fun) {
  CLI::Option *Opt = App.add_option(Name, Fun, Description, Defaulted);
  const auto RequireEvenNumberOfPairs = -2;
  Opt->type_name("KEY VALUE");
  Opt->type_size(RequireEvenNumberOfPairs);
  return Opt;
}

CLI::Option *addKafkaOption(CLI::App &App, std::string const &Name,
                            std::map<std::string, std::string> &ConfigMap,
                            std::string const &Description,
                            bool Defaulted = false) {
  CLI::callback_t Fun = [&ConfigMap](CLI::results_t Results) {
    for (size_t i = 0; i < Results.size() / 2; i++) {
      ConfigMap[Results.at(i * 2)] = Results.at(i * 2 + 1);
    }
    return true;
  };
  return SetKeyValueOptions(App, Name, Description, Defaulted, Fun);
}

bool parseLogLevel(std::vector<std::string> LogLevelString,
                   spdlog::level::level_enum &LogLevelResult) {
  std::map<std::string, spdlog::level::level_enum> LevelMap{
      {"Critical", spdlog::level::critical}, {"Error", spdlog::level::err},
      {"Warning", spdlog::level::warn},      {"Info", spdlog::level::info},
      {"Debug", spdlog::level::debug},       {"Trace", spdlog::level::trace}};

  if (LogLevelString.size() != 1) {
    return false;
  }
  try {
    LogLevelResult = LevelMap.at(LogLevelString.at(0));
    return true;
  } catch (std::out_of_range &e) {
    // Do nothing
  }
  try {
    int TempLogMessageLevel = std::stoi(LogLevelString.at(0));
    if (TempLogMessageLevel < 0 or TempLogMessageLevel > 5) {
      return false;
    }
    LogLevelResult = spdlog::level::level_enum(TempLogMessageLevel);
  } catch (std::invalid_argument &e) {
    return false;
  }

  return true;
}

void setCLIOptions(CLI::App &App, MainOpt &MainOptions) {
  App.add_flag("--version", MainOptions.PrintVersion,
               "Print application version and exit");

  // and add option for json config file instead
  App.add_option("--commands-json", MainOptions.CommandsJsonFilename,
                 "Specify a json file to set config")
      ->check(CLI::ExistingFile);

  addUriOption(
      App, "--command-uri", MainOptions.CommandBrokerURI,
      "<host[:port][/topic]> Kafka broker/topic to listen for commands")
      ->required();

  addUriOption(App, "--status-uri", MainOptions.KafkaStatusURI,
               MainOptions.ReportStatus,
               "<host[:port][/topic]> Kafka broker/topic to publish status "
               "updates on");
  addUriOption(App, "--graylog-logger-address",
               MainOptions.GraylogLoggerAddress,
               "<host:port> Log to Graylog via graylog_logger library");
  std::string LogLevelInfoStr =
      R"*(Set log message level. Set to 0 - 5 or one of
  `Trace`, `Debug`, `Info`, `Warning`, `Error`
  or `Critical`. Ex: "-v Debug". Default: `Error`)*";
  App.add_option(
      "-v,--verbosity",
      [&MainOptions, LogLevelInfoStr](std::vector<std::string> Input) {
        return parseLogLevel(Input, MainOptions.LoggingLevel);
      },
      LogLevelInfoStr, true);
  App.add_option("--hdf-output-prefix", MainOptions.HDFOutputPrefix,
                 "<absolute/or/relative/directory> Directory which gets "
                 "prepended to the HDF output filenames in the file write "
                 "commands");
  App.add_flag("--logpid-sleep", MainOptions.logpid_sleep);
  App.add_flag("--use-signal-handler", MainOptions.use_signal_handler);
  App.add_option("--log-file", MainOptions.LogFilename,
                 "Specify file to log to");
  App.add_option("--teamid", MainOptions.teamid);
  App.add_option("--service-id", MainOptions.ServiceID,
                 "Identifier string for this filewriter instance. Otherwise by "
                 "default a string containing hostname and process id.");
  App.add_flag("--list_modules", MainOptions.ListWriterModules,
               "List registered read and writer parts of file-writing modules"
               " and then exit.");
  addMillisecondOption(App, "--status-master-interval", MainOptions.StatusMasterIntervalMS,
                 "Interval in milliseconds for status updates", true);
  addMillisecondOption(App, "--streamer-ms-before-start",
                       MainOptions.StreamerConfiguration.BeforeStartTime,
                       "Streamer option - milliseconds before start time",
                       true);
  addMillisecondOption(App, "--streamer-ms-after-stop",
                       MainOptions.StreamerConfiguration.AfterStopTime,
                       "Streamer option - milliseconds after stop time", true);
  addMillisecondOption(App, "--streamer-start-time",
                       MainOptions.StreamerConfiguration.StartTimestamp,
                       "Streamer option - start timestamp (milliseconds)",
                       true);
  addMillisecondOption(App, "--streamer-stop-time",
                       MainOptions.StreamerConfiguration.StopTimestamp,
                       "Streamer option - stop timestamp (milliseconds)", true);
  addMillisecondOption(
      App, "--stream-master-topic-write-interval",
      MainOptions.topic_write_duration,
      "Stream-master option - topic write interval (milliseconds)");
  addKafkaOption(
      App, "-S,--kafka-config",
      MainOptions.StreamerConfiguration.BrokerSettings.KafkaConfiguration,
      "LibRDKafka options");
  App.set_config("-c,--config-file", "", "Read configuration from an ini file");
}
