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

std::string addLineBreaks(std::string Input) {
  const unsigned int MaxCharacters{50};
  unsigned int Beginning{0};
  while (Beginning + MaxCharacters < Input.size()) {
    auto CString = Input.substr(Beginning, MaxCharacters);
    auto ModifyLoc = Beginning + CString.find_last_of(' ');
    Input[ModifyLoc] = '\n';
    Beginning = ModifyLoc;
  }
  return Input;
}

CLI::Option *uriOption(CLI::App &App, const std::string &Name,
                       CLI::callback_t Fun, const std::string &Description,
                       bool Defaulted) {

  CLI::Option *Opt = App.add_option(Name, std::move(Fun),
                                    addLineBreaks(Description), Defaulted);
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
  App.add_option(Name, Fun, addLineBreaks(Description), Defaulted);
}

CLI::Option *SetKeyValueOptions(CLI::App &App, const std::string &Name,
                                const std::string &Description, bool Defaulted,
                                const CLI::callback_t &Fun) {
  CLI::Option *Opt =
      App.add_option(Name, Fun, addLineBreaks(Description), Defaulted);
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
  return SetKeyValueOptions(App, Name, addLineBreaks(Description), Defaulted,
                            Fun);
}

bool parseLogLevel(std::vector<std::string> LogLevelString,
                   LogSeverity &LogLevelResult) {
  auto ToLower = [](auto InString) {
    std::transform(InString.begin(), InString.end(), InString.begin(),
                   [](auto C) { return std::tolower(C); });
    return InString;
  };
  std::map<std::string, LogSeverity> LevelMap{
      {"critical", LogSeverity::Critical}, {"error", LogSeverity::Error},
      {"warning", LogSeverity::Warn},      {"info", LogSeverity::Info},
      {"debug", LogSeverity::Debug},       {"trace", LogSeverity::Trace}};

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
    LogLevelResult = LogSeverity(TempLogMessageLevel);
  } catch (std::invalid_argument &e) {
    return false;
  }

  return true;
}

void setCLIOptions(CLI::App &App, MainOpt &MainOptions) {
  App.add_flag("--version", MainOptions.PrintVersion,
               "Print application version and exit");

  App.add_option("--brokers", MainOptions.brokers,
                 "Comma separated list of Kafka brokers")
      ->required();

  App.add_option("--command-status-topic", MainOptions.command_topic,
                 "Kafka topic to listen for "
                 "commands and to push status updates to")
      ->required();

  App.add_option("--job-pool-topic", MainOptions.job_pool_topic,
                 "Kafka topic to listen for jobs on")
      ->required();

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
  App.add_option(
      "--hdf-output-prefix", MainOptions.HDFOutputPrefix,
      addLineBreaks("Relative or absolute path to directory which gets "
                    "prepended to the HDF output filenames in the file write "
                    "commands. Default: current working directory"));
  App.add_option("--log-file", MainOptions.LogFilename,
                 "Specify file to log to");
  App.add_option("--server-status-port", MainOptions.ServerStatusPort,
                 "TCP Port");
  App.add_option(
      "--max-queued-writes", MainOptions.StreamerConfiguration.MaxQueuedWrites,
      addLineBreaks(
          "Maximum number of messages buffered for writing. Directly "
          "affects the memory usage of the application. The maximum is "
          "not enforced, only used as guideline to throttle Kafka "
          "consumption. Note that total memory usage will also depend on "
          "the size of the actual messages consumed from Kafka."));
  App.add_option(
         "--service-name",
         [&MainOptions](std::vector<std::string> ServiceNames) -> bool {
           MainOptions.setServiceName(ServiceNames.back());
           return true;
         },
         addLineBreaks("Used to generate the service identifier and as an "
                       "extra metrics ID "
                       "string."
                       "Will make the metrics names take the form: "
                       "\"kafka-to-nexus.[host-name].[service-name].*\""))
      ->default_str(MainOpt::getDefaultServiceId());
  App.add_flag(
      "--list_modules", MainOptions.ListWriterModules,
      addLineBreaks(
          "List registered read and writer parts of file-writing modules"
          " and then exit."));
  addDurationOption(
      App, "--status-master-interval", MainOptions.StatusMasterInterval,
      R"(Interval between status updates.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  addDurationOption(
      App, "--time-before-start",
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
      R"(Amount of time to wait for recovery from kafka error before abandoning stream. Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  addDurationOption(
      App, "--kafka-poll-timeout",
      MainOptions.StreamerConfiguration.BrokerSettings.PollTimeout,
      R"(Amount of time to wait for new kafka message. *WARNING* Should generally not be changed from the default. Increase the "--kafka-error-timeout" instead.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
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
