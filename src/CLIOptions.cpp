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
#include <regex>

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

void addDurationOption(CLI::App &App, const std::string &Name, duration &MSArg,
                       const std::string &Description = "",
                       bool Defaulted = false) {
  CLI::callback_t Fun = [&MSArg](CLI::results_t Results) {
    std::regex const TimeRe{"(\\d+\\.?\\d*)\\s?(ms|min|m|h|sec|s)*$"};
    std::smatch Match;
    if (not std::regex_match(Results[0], Match, TimeRe)) {
      return false;
    }
    int TotalMilliseconds{0};
    std::string FoundUnit{Match[2]};
    using std::string_literals::operator""s;
    if (FoundUnit == "s"s or FoundUnit == "sec"s or FoundUnit == ""s) {
      TotalMilliseconds = std::round(std::stod(Match[1]) * 1000);
    } else if (FoundUnit == "ms"s) {
      TotalMilliseconds = std::round(std::stod(Match[1]));
    } else if (FoundUnit == "m"s or FoundUnit == "min"s) {
      TotalMilliseconds = std::round(std::stod(Match[1]) * 1000 * 60);
    } else if (FoundUnit == "h"s) {
      TotalMilliseconds = std::round(std::stod(Match[1]) * 1000 * 60 * 60);
    } else {
      return false;
    }
    MSArg = std::chrono::milliseconds(TotalMilliseconds);
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
                   Log::Severity &LogLevelResult) {
  auto ToLower = [](auto InString) {
    std::transform(InString.begin(), InString.end(), InString.begin(),
                   [](auto C) { return std::tolower(C); });
    return InString;
  };
  std::map<std::string, Log::Severity> LevelMap{
      {"critical", Log::Severity::Critical}, {"error", Log::Severity::Error},
      {"warning", Log::Severity::Warning},   {"info", Log::Severity::Info},
      {"debug", Log::Severity::Debug},       {"trace", Log::Severity::Trace}};

  if (LogLevelString.size() != 1) {
    return false;
  }
  try {
    LogLevelResult = LevelMap.at(ToLower(LogLevelString.at(0)));
    return true;
  } catch (std::out_of_range &e) {
    // Do nothing
  }
  try {
    int TempLogMessageLevel = std::stoi(LogLevelString.at(0));
    if (TempLogMessageLevel < 0 or TempLogMessageLevel > 6) {
      return false;
    }
    LogLevelResult = Log::Severity(TempLogMessageLevel);
  } catch (std::invalid_argument &e) {
    return false;
  }

  return true;
}

void setCLIOptions(CLI::App &App, MainOpt &MainOptions) {
  App.add_flag("--version", MainOptions.PrintVersion,
               "Print application version and exit");

  addUriOption(App, "--command-status-uri", MainOptions.CommandBrokerURI,
               "<host[:port][/topic]> Kafka broker/topic to listen for "
               "commands and to push status updates to.")
      ->required();

  addUriOption(App, "--job-pool-uri", MainOptions.JobPoolURI,
               "<host[:port][/topic]> Kafka broker/topic to listen for jobs")
      ->required();

  addUriOption(App, "--graylog-logger-address",
               MainOptions.GraylogLoggerAddress,
               "<host:port> Log to Graylog via graylog_logger library");
  addUriOption(App, "--grafana-carbon-address",
               MainOptions.GrafanaCarbonAddress,
               "<host:port> Address to the Grafana (Carbon) metrics service.");
  std::string LogLevelInfoStr =
      R"*(Set log message level. Set to 0 - 5 or one of
  `Debug`, `Info`, `Warning`, `Error`
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
  App.add_option("--log-file", MainOptions.LogFilename,
                 "Specify file to log to");
  App.add_option(
         "--service-name",
         [&MainOptions](std::vector<std::string> ServiceNames) -> bool {
           MainOptions.setServiceName(ServiceNames.back());
           return true;
         },
         "Used to generate the service identifier and as an extra metrics ID "
         "string."
         "Will make the metrics names take the form: "
         "\"kafka-to-nexus.[host-name].[service-name].*\"")
      ->default_str(MainOpt::getDefaultServiceId());
  App.add_flag("--list_modules", MainOptions.ListWriterModules,
               "List registered read and writer parts of file-writing modules"
               " and then exit.");
  addDurationOption(App, "--status-master-interval",
                    MainOptions.StatusMasterInterval,
                    R"(Interval between status updates.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
                    true);
  addDurationOption(App, "--time-before-start",
                    MainOptions.StreamerConfiguration.BeforeStartTime,
                    R"(Pre-consume messages this amount of time.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
                    true);
  addDurationOption(
      App, "--time-after-stop", MainOptions.StreamerConfiguration.AfterStopTime,
      R"(Allow for this much leeway after stop time before stopping message consumption.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  addDurationOption(
      App, "--kafka-metadata-max-timeout",
      MainOptions.StreamerConfiguration.BrokerSettings.MaxMetadataTimeout,
      R"(Max timeout for kafka metadata calls. Note: metadata calls block the application. Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  addDurationOption(
      App, "--kafka-error-timeout",
      MainOptions.StreamerConfiguration.BrokerSettings.KafkaErrorTimeout,
      R"(Number of seconds to wait for recovery from kafka error before abandoning stream. Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  addDurationOption(
      App, "--data-flush-interval",
      MainOptions.StreamerConfiguration.DataFlushInterval,
      R"((Max) amount of time between flushing of data to file, in seconds.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  addKafkaOption(
      App, "-X,--kafka-config",
      MainOptions.StreamerConfiguration.BrokerSettings.KafkaConfiguration,
      "LibRDKafka options");
  App.set_config("-c,--config-file", "", "Read configuration from an ini file");
}
