#include "kafka-to-nexus.h"
#include "MainOpt.h"
#include "Master.h"
#include "logger.h"
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
  auto po = parse_opt(argc, argv);
  auto opt = std::move(po.second);
  // For the signal handler
  g_main_opt.store(opt.get());

  fmt::print("kafka-to-nexus {:.7} (ESS, BrightnESS)\n", GIT_COMMIT);
  fmt::print("  Contact: dominik.werder@psi.ch, michele.brambilla@psi.ch\n\n");

  if (opt->help) {
    fmt::print(
        "Forwards EPICS process variables to Kafka topics.\n"
        "Controlled via JSON packets sent over the configuration topic.\n"
        "\n"
        "\n"
        "kafka-to-nexus\n"
        "  --help, -h\n"
        "\n"
        "  --config-file               <filename.json>\n"
        "\n"
        "  --command-uri               <//host[:port][/topic]>\n"
        "      Kafka broker/topic to listen for commands.\n"
        "      Default: //{}/{}\n"
        "      Legacy alias: --broker-command\n"
        "\n"
        "  --status-uri                <//host[:port][/topic]>\n"
        "      Kafka broker/topic to publish status updates on.\n"
        "      Default: //{}/{}\n"
        "\n",
        opt->command_broker_uri.host_port, opt->command_broker_uri.topic,
        opt->kafka_status_uri.host_port, opt->kafka_status_uri.topic);

    fmt::print("  --kafka-gelf                <//host[:port]/topic>\n"
               "      Log to Graylog via Kafka GELF adapter.\n"
               "\n");

    fmt::print("  --graylog-logger-address    <host:port>\n"
               "      Log to Graylog via graylog_logger library.\n"
               "\n");

    fmt::print(
        "  -v\n"
        "      Set logging level. 3 == Error, 7 == Debug. Default: 6 (Info).\n"
        "\n");
    return 1;
  }

  if (po.first) {
    return 1;
  }

  if (opt->use_signal_handler) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
  }

  setup_logger_from_options(*opt);

  FileWriter::Master m(*opt);
  opt->master = &m;
  std::thread t1([&m] { m.run(); });
  t1.join();
  opt->master = nullptr;

  return 0;
}
