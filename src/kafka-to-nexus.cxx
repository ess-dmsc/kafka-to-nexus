#include "kafka-to-nexus.h"
#include "MainOpt.h"
#include "Master.h"
#include "logger.h"
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>

// These should only be visible in this translation unit
static std::atomic_bool GotSignal{false};
static std::atomic_int SignalId{0};

void signal_handler(int Signal) {
  GotSignal = true;
  SignalId = Signal;
}

int main(int argc, char **argv) {
  auto po = parse_opt(argc, argv);
  auto opt = std::move(po.second);
  opt->init();

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

    fmt::print(
        "  --hdf-output-prefix <absolute/or/relative/directory>\n"
        "      Directory which gets prepended to the HDF output filenames in\n"
        "      the file write commands.\n"
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

  FileWriter::Master Master(*opt);
  std::thread MasterThread([&Master] { Master.run(); });

  while (not Master.RunLoopExited()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (GotSignal) {
      LOG(Sev::Notice, "SIGNAL {}", SignalId);
      Master.stop();
      GotSignal = false;
      break;
    }
  }

  MasterThread.join();
  return 0;
}
