// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferReader.h"
#include "HDFVersionCheck.h"
#include "JobCreator.h"
#include "Metrics/CarbonSink.h"
#include "Metrics/LogSink.h"
#include "Metrics/Registrar.h"
#include "Metrics/Reporter.h"
#include "RunState.h"
#include "Status/StatusInfo.h"
#include "Status/StatusReporter.h"
#include "Version.h"
#include "WriterRegistrar.h"
#include <CLI/CLI.hpp>
#include <regex>
#include <utility>

// This should only be visible in this translation unit
static std::atomic<RunStates> RUN_STATE{RunStates::Running};

std::unique_ptr<Status::StatusReporter>
create_status_reporter(MainOpt const &options, std::string const &app_name,
                       std::string const &app_version) {
  Kafka::BrokerSettings broker_settings =
      options.StreamerConfiguration.BrokerSettings;
  auto const status_info =
      Status::ApplicationStatusInfo{options.StatusMasterInterval,
                                    app_name,
                                    app_version,
                                    getHostName(),
                                    options.ServiceName,
                                    options.getServiceId(),
                                    getPID()};
  return std::make_unique<Status::StatusReporter>(
      broker_settings, options.command_topic, status_info);
}

bool try_to_find_topics(std::string const &pool_topic,
                        std::string const &command_topic, duration timeout,
                        Kafka::BrokerSettings const &broker_settings) {
  try {
    auto topics = Kafka::MetadataEnquirer().getTopicList(
        broker_settings.Address, timeout, broker_settings);
    if (topics.find(pool_topic) == topics.end()) {
      auto message = fmt::format(
          R"(Unable to find job pool topic with name "{}".)", pool_topic);
      Logger::Critical(message);
      throw std::runtime_error(message);
    }
    if (topics.find(command_topic) == topics.end()) {
      auto message = fmt::format(
          R"(Unable to find command topic with name "{}".)", command_topic);
      Logger::Critical(message);
      throw std::runtime_error(message);
    }
  } catch (MetadataException const &error) {
    Logger::Info("Meta data query failed with message: {}", error.what());
    return false;
  }
  return true;
}

std::string vector_to_comma_separated_string(std::vector<std::string> vector) {
  return std::accumulate(
      std::next(vector.begin()), vector.end(), vector.at(0),
      [](std::string &a, std::string &b) { return a + "," + (b); });
}

std::string wrap_lines(std::string input) {
  const unsigned int max_chars{50};
  unsigned int beginning{0};
  while (beginning + max_chars < input.size()) {
    auto sub_string = input.substr(beginning, max_chars);
    auto split_location = beginning + sub_string.find_last_of(' ');
    input[split_location] = '\n';
    beginning = split_location;
  }
  return input;
}

void add_duration_option(CLI::App &app, const std::string &name,
                         duration &value, const std::string &description = "",
                         bool defaulted = false) {
  CLI::callback_t callback = [&value](CLI::results_t results) {
    std::regex const regex{R"((\d+\.?\d*)\s?(ms|min|m|h|sec|s)*$)"};
    std::smatch match;
    if (!std::regex_match(results[0], match, regex)) {
      return false;
    }
    int total_milliseconds{0};
    std::string found_unit{match[2]};
    using std::string_literals::operator""s;
    if (found_unit == "s"s || found_unit == "sec"s || found_unit.empty()) {
      total_milliseconds = std::round(std::stod(match[1]) * 1000);
    } else if (found_unit == "ms"s) {
      total_milliseconds = std::round(std::stod(match[1]));
    } else if (found_unit == "m"s || found_unit == "min"s) {
      total_milliseconds = std::round(std::stod(match[1]) * 1000 * 60);
    } else if (found_unit == "h"s) {
      total_milliseconds = std::round(std::stod(match[1]) * 1000 * 60 * 60);
    } else {
      return false;
    }
    value = std::chrono::milliseconds(total_milliseconds);
    return true;
  };
  app.add_option(name, callback, wrap_lines(description), defaulted);
}

CLI::Option *add_uri_option(CLI::App &app, std::string const &name,
                            uri::URI &uri, std::string const &description = "",
                            bool defaulted = false) {
  CLI::callback_t callback = [&uri](CLI::results_t results) {
    try {
      uri.parse(results[0]);
    } catch (std::runtime_error &E) {
      return false;
    }
    return true;
  };

  CLI::Option *opt = app.add_option(name, std::move(callback),
                                    wrap_lines(description), defaulted);
  opt->type_name("URI");
  opt->type_size(1);
  return opt;
}

CLI::Option *add_kafka_option(CLI::App &app, std::string const &name,
                              std::map<std::string, std::string> &config_map,
                              std::string const &description,
                              bool defaulted = false) {
  CLI::callback_t callback = [&config_map](CLI::results_t results) {
    for (size_t i = 0; i < results.size() / 2; i++) {
      config_map[results.at(i * 2)] = results.at(i * 2 + 1);
    }
    return true;
  };
  CLI::Option *opt =
      app.add_option(name, callback, wrap_lines(description), defaulted);
  opt->type_name("KEY VALUE");
  // -2 => must be a pair (key, value)
  opt->type_size(-2);
  return opt;
}

bool parse_log_level(std::vector<std::string> input, LogSeverity &result) {
  auto to_lower = [](auto input) {
    std::transform(input.begin(), input.end(), input.begin(),
                   [](auto c) { return std::tolower(c); });
    return input;
  };
  std::map<std::string, LogSeverity> string_to_level{
      {"critical", LogSeverity::Critical}, {"error", LogSeverity::Error},
      {"warning", LogSeverity::Warn},      {"info", LogSeverity::Info},
      {"debug", LogSeverity::Debug},       {"trace", LogSeverity::Trace}};

  if (input.size() != 1) {
    return false;
  }
  try {
    result = string_to_level.at(to_lower(input.at(0)));
    return true;
  } catch (std::out_of_range &e) {
    // Do nothing
  }
  try {
    int temp_level = std::stoi(input.at(0));
    if (temp_level < 0 || temp_level > 6) {
      return false;
    }
    result = LogSeverity(temp_level);
  } catch (std::invalid_argument &e) {
    return false;
  }
  return true;
}

int main(int argc, char **argv) {
  std::string const app_name = "kafka-to-nexus";
  std::string const app_version = GetVersion();
  CLI::App app{fmt::format("{} {:.7} (European Spallation Source ERIC)\n"
                           "https://github.com/ess-dmsc/kafka-to-nexus\n",
                           app_name, app_version)};
  auto options = std::make_unique<MainOpt>();
  app.add_flag("--version", options->PrintVersion,
               "Print application version and exit");

  app.add_option("--brokers", options->brokers,
                 "Comma separated list of Kafka brokers")
      ->required();

  app.add_option("--command-status-topic", options->command_topic,
                 "Kafka topic to listen for "
                 "commands and to push status updates to")
      ->required();

  app.add_option("--job-pool-topic", options->job_pool_topic,
                 "Kafka topic to listen for jobs on")
      ->required();

  add_uri_option(
      app, "--grafana-carbon-address", options->GrafanaCarbonAddress,
      "<host:port> Address to the Grafana (Carbon) metrics service.");
  std::string log_level_info =
      R"*(Set log message level. Set to 0 - 5 or one of
  `Debug`, `Info`, `Warning`, `Error`
  or `Critical`. Ex: "-v Debug". Default: `Error`)*";
  app.add_option(
      "-v,--verbosity",
      [&options, log_level_info](std::vector<std::string> input) {
        return parse_log_level(std::move(input), options->LoggingLevel);
      },
      log_level_info, true);
  app.add_option(
      "--hdf-output-prefix", options->HDFOutputPrefix,
      wrap_lines("Relative or absolute path to directory which gets "
                 "prepended to the HDF output filenames in the file write "
                 "commands. Default: current working directory"));
  app.add_option(
      "--hdf-template-prefix", options->HDFTemplatePrefix,
      wrap_lines("Relative or absolute path to directory which gets "
                 "prepended to the HDF template filenames in the file write "
                 "commands. Default: current working directory"));
  app.add_option(
      "--max-queued-writes", options->StreamerConfiguration.MaxQueuedWrites,
      wrap_lines(
          "Maximum number of messages buffered for writing. Directly "
          "affects the memory usage of the application. The maximum is "
          "not enforced, only used as guideline to throttle Kafka "
          "consumption. Note that total memory usage will also depend on "
          "the size of the actual messages consumed from Kafka."));
  app.add_option(
         "--service-name",
         [&options](std::vector<std::string> service_names) -> bool {
           options->setServiceName(service_names.back());
           return true;
         },
         wrap_lines("Used to generate the service identifier and as an "
                    "extra metrics ID "
                    "string."
                    "Will make the metrics names take the form: "
                    "\"kafka-to-nexus.[host-name].[service-name].*\""))
      ->default_str(MainOpt::getDefaultServiceId());
  add_duration_option(
      app, "--status-master-interval", options->StatusMasterInterval,
      R"(Interval between status updates.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_duration_option(
      app, "--time-before-start",
      options->StreamerConfiguration.BeforeStartTime,
      R"(Pre-consume messages this amount of time.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_duration_option(
      app, "--time-after-stop", options->StreamerConfiguration.AfterStopTime,
      R"(Allow for this much leeway after stop time before stopping message consumption.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_duration_option(
      app, "--kafka-metadata-max-timeout",
      options->StreamerConfiguration.BrokerSettings.MaxMetadataTimeout,
      R"(Max timeout for kafka metadata calls. Note: metadata calls block the application. Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_duration_option(
      app, "--kafka-error-timeout",
      options->StreamerConfiguration.BrokerSettings.KafkaErrorTimeout,
      R"(Amount of time to wait for recovery from kafka error before abandoning stream. Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_duration_option(
      app, "--kafka-poll-timeout",
      options->StreamerConfiguration.BrokerSettings.PollTimeout,
      R"(Amount of time to wait for new kafka message. *WARNING* Should generally not be changed from the default. Increase the "--kafka-error-timeout" instead.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_duration_option(
      app, "--data-flush-interval",
      options->StreamerConfiguration.DataFlushInterval,
      R"((Max) amount of time between flushing of data to file, in seconds.  Ex. "10s". Accepts "h", "m", "s" and "ms".)",
      true);
  add_kafka_option(
      app, "-X,--kafka-config",
      options->StreamerConfiguration.BrokerSettings.KafkaConfiguration,
      "LibRDKafka options");
  app.set_config("-c,--config-file", "", "Read configuration from an ini file");

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &error) {
    // Do nothing, we only care about the version flag in this first pass.
  }

  if (options->PrintVersion) {
    fmt::print("{}\n", app_version);
    return EXIT_SUCCESS;
  }
  app.clear();

  CLI11_PARSE(app, argc, argv);
  setupLoggerFromOptions(*options);
  if (!versionOfHDF5IsOk()) {
    Logger::Critical("Failed HDF5 version check. Exiting.");
    return EXIT_FAILURE;
  }

  using std::chrono_literals::operator""ms;
  std::vector<std::shared_ptr<Metrics::Reporter>> metric_reporters;
  metric_reporters.push_back(std::make_shared<Metrics::Reporter>(
      std::make_unique<Metrics::LogSink>(), 60s));

  if (!options->GrafanaCarbonAddress.HostPort.empty()) {
    metric_reporters.push_back(std::make_shared<Metrics::Reporter>(
        std::make_unique<Metrics::CarbonSink>(
            options->GrafanaCarbonAddress.Host,
            options->GrafanaCarbonAddress.Port),
        10s));
  }

  auto fqdn = getFQDN();
  std::replace(fqdn.begin(), fqdn.end(), '.', '_');
  std::string metric_prefix = app_name + "." + fqdn;

  std::string prefix_service_component;
  if (options->ServiceName.empty()) {
    prefix_service_component = options->getServiceId();
  } else {
    prefix_service_component = options->ServiceName;
  }
  metric_prefix += "." + prefix_service_component;
  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<Metrics::Registrar>(metric_prefix, metric_reporters);

  std::signal(SIGHUP, [](int signal) { signal_handler(signal, RUN_STATE); });
  std::signal(SIGINT, [](int signal) { signal_handler(signal, RUN_STATE); });
  std::signal(SIGTERM, [](int signal) { signal_handler(signal, RUN_STATE); });

  options->StreamerConfiguration.BrokerSettings.Address =
      vector_to_comma_separated_string(options->brokers);

  std::unique_ptr<FileWriter::Master> master;

  auto generate_master = [&]() {
    return std::make_unique<FileWriter::Master>(
        *options,
        Command::Handler::create(options->getServiceId(),
                                 options->StreamerConfiguration.BrokerSettings,
                                 options->job_pool_topic,
                                 options->command_topic),
        create_status_reporter(*options, app_name, app_version),
        std::move(registrar));
  };

  bool find_topic_mode{true};
  duration metadata_timeout{
      options->StreamerConfiguration.BrokerSettings.MinMetadataTimeout};
  Logger::Debug("Starting run loop.");
  Logger::Debug("Retrieving topic names from broker.");
  while (!shouldStop(master, find_topic_mode, RUN_STATE)) {
    try {
      if (find_topic_mode) {
        if (try_to_find_topics(options->job_pool_topic, options->command_topic,
                               metadata_timeout,
                               options->StreamerConfiguration.BrokerSettings)) {
          Logger::Debug("Command and status topics found, starting master.");
          master = generate_master();
          find_topic_mode = false;
        } else {
          metadata_timeout *= 2;
          if (metadata_timeout > options->StreamerConfiguration.BrokerSettings
                                     .MaxMetadataTimeout) {
            metadata_timeout = options->StreamerConfiguration.BrokerSettings
                                   .MaxMetadataTimeout;
          }
          Logger::Info(
              R"(Meta data call for retrieving the command topic ("{}") from the broker failed. Re-trying with a timeout of {} ms.)",
              options->command_topic,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  metadata_timeout)
                  .count());
        }
      } else {
        master->run();
      }
    } catch (std::system_error const &e) {
      Logger::Critical(
          "std::system_error  code: {}  category: {}  message: {}  what: {}",
          e.code().value(), e.code().category().name(), e.code().message(),
          e.what());
      break;
    } catch (std::runtime_error const &e) {
      Logger::Critical("std::runtime_error  what: {}", e.what());
      break;
    } catch (std::exception const &e) {
      Logger::Critical("std::exception  what: {}", e.what());
      break;
    }
  }
  if (RUN_STATE == RunStates::SIGINT_KafkaWait) {
    Logger::Debug("Giving a grace period to Kafka.");
    std::this_thread::sleep_for(3s);
  }
  Logger::Info("Exiting.");
  Logger::Flush();
  return EXIT_SUCCESS;
}
