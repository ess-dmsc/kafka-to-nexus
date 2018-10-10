#include "CLIOptions.h"
#include "MainOpt.h"
#include "uri.h"
#include <CLI/CLI.hpp>

CLI::Option *uriOption(CLI::App &App, const std::string &Name, uri::URI &URIArg,
                       CLI::callback_t Fun, const std::string &Description,
                       bool Defaulted) {

  CLI::Option *Opt = App.add_option(Name, Fun, Description, Defaulted);
  Opt->set_custom_option("URI", 1);
  if (Defaulted) {
    Opt->set_default_str(URIArg.getURIString());
  }
  return Opt;
}

/// Use for adding a URI option
CLI::Option *addOption(CLI::App &App, std::string Name, uri::URI &URIArg,
                       std::string Description = "", bool Defaulted = false) {
  CLI::callback_t Fun = [&URIArg](CLI::results_t Results) {
    URIArg.parse(Results[0]);
    return true;
  };

  return uriOption(App, Name, URIArg, Fun, Description, Defaulted);
}

/// Use for adding a URI option, if the URI is given then TrueIfOptionGiven is
/// set to true
CLI::Option *addOption(CLI::App &App, const std::string &Name, uri::URI &URIArg,
                       bool &TrueIfOptionGiven,
                       const std::string &Description = "",
                       bool Defaulted = false) {
  CLI::callback_t Fun = [&URIArg, &TrueIfOptionGiven](CLI::results_t Results) {
    TrueIfOptionGiven = true;
    URIArg.parse(Results[0]);
    return true;
  };

  return uriOption(App, Name, URIArg, Fun, Description, Defaulted);
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
  Opt->set_custom_option("KEY VALUE", RequireEvenNumberOfPairs);
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

void setCLIOptions(CLI::App &App, MainOpt &MainOptions) {
  // and add option for json config file instead
  App.add_option("--commands-json", MainOptions.CommandsJsonFilename,
                 "Specify a json file to set config")
      ->check(CLI::ExistingFile);

  addOption(App, "--command-uri", MainOptions.command_broker_uri,
            "<//host[:port][/topic]> Kafka broker/topic to listen for commands")
      ->required();
  addOption(App, "--status-uri", MainOptions.kafka_status_uri,
            MainOptions.do_kafka_status,
            "<//host[:port][/topic]> Kafka broker/topic to publish status "
            "updates on");
  App.add_option("--kafka-gelf", MainOptions.kafka_gelf,
                 "<//host[:port]/topic> Log to Graylog via Kafka GELF adapter");
  App.add_option("--graylog-logger-address", MainOptions.graylog_logger_address,
                 "<host:port> Log to Graylog via graylog_logger library");
  App.add_option(
         "-v,--verbosity", log_level,
         "Set logging level. 3 == Error, 7 == Debug. Default: 3 (Error)", true)
      ->check(CLI::Range(1, 7));
  App.add_option("--hdf-output-prefix", MainOptions.hdf_output_prefix,
                 "<absolute/or/relative/directory> Directory which gets "
                 "prepended to the HDF output filenames in the file write "
                 "commands");
  App.add_flag("--logpid-sleep", MainOptions.logpid_sleep);
  App.add_flag("--use-signal-handler", MainOptions.use_signal_handler);
  App.add_option("--teamid", MainOptions.teamid);
  App.add_option("--service-id", MainOptions.service_id,
                 "Identifier string for this filewriter instance. Otherwise by "
                 "default a string containing hostname and process id.");
  App.add_option("--status-master-interval", MainOptions.status_master_interval,
                 "Interval in milliseconds for status updates", true);
  App.add_flag("--list_modules", MainOptions.ListWriterModules,
               "List registered read and writer parts of file-writing modules"
               " and then exit.");
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
  addKafkaOption(App, "-S,--kafka-config",
                 MainOptions.StreamerConfiguration.Settings.KafkaConfiguration,
                 "LibRDKafka options");
  App.set_config("-c,--config-file", "", "Read configuration from an ini file");
}
