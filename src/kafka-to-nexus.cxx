#include "CLIOptions.h"
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
