#include "kafka-to-nexus.h"
#include "MainOpt.h"
#include "Master.h"
#include "logger.h"
#include "uri.h"
#include <CLI/CLI.hpp>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>

CLI::Option *add_option(CLI::App &App, std::string Name, uri::URI &Variable,
                        std::string Description = "", bool Defaulted = false) {
  CLI::callback_t Fun = [&Variable](CLI::results_t Results) {
    Variable.parse(Results[0]);
    return true;
  };

  CLI::Option *Opt = App.add_option(Name, Fun, Description, Defaulted);
  Opt->set_custom_option("URI", 1);
  if (Defaulted) {
    Opt->set_default_str(Variable.URIString);
  }
  return Opt;
}

void setCLIOptions(CLI::App &App, MainOpt &MainOptions) {
  App.set_config("--config-file", "", "Specify an ini file to set config",
                 false);
  add_option(
      App, "--command-uri", MainOptions.command_broker_uri,
      "<//host[:port][/topic]> Kafka broker/topic to listen for commands");
  add_option(App, "--status-uri", MainOptions.kafka_status_uri,
             "<//host[:port][/topic]> Kafka broker/topic to publish status "
             "updates on");
  App.add_option("--kafka-gelf", MainOptions.kafka_gelf,
                 "<//host[:port]/topic> Log to Graylog via Kafka GELF adapter");
  App.add_option("--graylog-logger-address", MainOptions.graylog_logger_address,
                 "<host:port> Log to Graylog via graylog_logger library");
  App.add_set("-v", log_level, {1, 2, 3, 4, 5, 6, 7},
              "Set logging level. 3 == Error, 7 == Debug. Default: 6 (Info)",
              true);
  App.add_option("--hdf-output-prefix", MainOptions.hdf_output_prefix,
                 "<absolute/or/relative/directory> Directory which gets "
                 "prepended to the HDF output filenames in the file write "
                 "commands");
  App.add_flag("--logpid-sleep", MainOptions.logpid_sleep);
  App.add_flag("--use-signal-handler", MainOptions.use_signal_handler);
  App.add_option("--teamid", MainOptions.teamid);
}

void signal_handler(int signal) {
  LOG(Sev::Notice, "SIGNAL {}", signal);
  if (auto opt = g_main_opt.load()) {
    if (auto m = opt->master.load()) {
      m->stop();
    }
  }
}

int main(int argc, char **argv) {

  fmt::print("kafka-to-nexus {:.7} (ESS, BrightnESS)\n"
             "  Contact: dominik.werder@psi.ch, michele.brambilla@psi.ch\n\n",
             GIT_COMMIT);
  CLI::App App{
      "Forwards EPICS process variables to Kafka topics.\n"
      "Controlled via JSON packets sent over the configuration topic.\n"};
  auto Options = std::unique_ptr<MainOpt>(new MainOpt());
  setCLIOptions(App, *Options);

  CLI11_PARSE(App, argc, argv);

  // For the signal handler
  g_main_opt.store(Options.get());

  if (Options->use_signal_handler) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
  }

  setup_logger_from_options(*Options);

  FileWriter::Master m(*Options);
  Options->master = &m;
  std::thread t1([&m] { m.run(); });
  t1.join();
  Options->master = nullptr;

  return 0;
}
