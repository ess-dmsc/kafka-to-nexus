// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CLIOptions.h"
#include "CommandListener.h"
#include "FlatbufferReader.h"
#include "JobCreator.h"
#include "MainOpt.h"
#include "Master.h"
#include "Metrics/CarbonSink.h"
#include "Metrics/LogSink.h"
#include "Metrics/Registrar.h"
#include "Metrics/Reporter.h"
#include "Status/StatusReporter.h"
#include "Version.h"
#include "WriterRegistrar.h"
#include "logger.h"
#include <CLI/CLI.hpp>
#include <csignal>
#include <string>

// These should only be visible in this translation unit
static std::atomic_bool GotSignal{false};
static std::atomic_int SignalId{0};

void signal_handler(int Signal) {
  GotSignal = true;
  SignalId = Signal;
}

std::unique_ptr<Status::StatusReporter>
createStatusReporter(MainOpt const &MainConfig) {
  Kafka::BrokerSettings BrokerSettings;
  BrokerSettings.Address = MainConfig.KafkaStatusURI.HostPort;
  auto StatusProducer = std::make_shared<Kafka::Producer>(BrokerSettings);
  auto StatusProducerTopic = std::make_unique<Kafka::ProducerTopic>(
      StatusProducer, MainConfig.KafkaStatusURI.Topic);
  return std::make_unique<Status::StatusReporter>(
      MainConfig.StatusMasterIntervalMS, MainConfig.ServiceID,
      StatusProducerTopic);
}

int main(int argc, char **argv) {
  CLI::App App{fmt::format(
      "kafka-to-nexus {:.7} (ESS, BrightnESS)\n"
      "https://github.com/ess-dmsc/kafka-to-nexus\n\n"
      "Writes NeXus files in a format specified with a json template.\n"
      "Writer modules can be used to populate the file from Kafka topics.\n",
      GetVersion())};
  auto Options = std::make_unique<MainOpt>();
  Options->init();
  setCLIOptions(App, *Options);

  try {
    App.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    // Do nothing, we only care about the version flag in this first pass.
  }

  if (Options->PrintVersion) {
    fmt::print("{}\n", GetVersion());
    return 0;
  }
  App.clear();

  CLI11_PARSE(App, argc, argv);
  setupLoggerFromOptions(*Options);
  auto Logger = getLogger();

  if (Options->ListWriterModules) {
    fmt::print("\n-- Known flatbuffer metadata extractors\n");
    for (auto &ReaderPair :
         FileWriter::FlatbufferReaderRegistry::getReaders()) {
      fmt::print("---- {}\n", ReaderPair.first);
    }
    fmt::print("\n--Known writer modules\n");
    for (auto &WriterPair : WriterModule::Registry::getFactoryIdsAndNames()) {
      fmt::print("---- {} : {}\n", WriterPair.first, WriterPair.second);
    }
    return 0;
  }
  using std::chrono_literals::operator""ms;
  std::vector<std::shared_ptr<Metrics::Reporter>> MetricsReporters;
  MetricsReporters.push_back(std::make_shared<Metrics::Reporter>(
      std::make_unique<Metrics::LogSink>(), 500ms));

  if (not Options->GrafanaCarbonAddress.HostPort.empty()) {
    auto HostName = Options->GrafanaCarbonAddress.Host;
    auto Port = Options->GrafanaCarbonAddress.Port;
    MetricsReporters.push_back(std::make_shared<Metrics::Reporter>(
        std::make_unique<Metrics::CarbonSink>(HostName, Port), 500ms));
  }

  Metrics::Registrar MainRegistrar("kakfa-to-nexus", MetricsReporters);
  auto UsedRegistrar = MainRegistrar.getNewRegistrar(Options->ServiceID);

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  FileWriter::Master Master(
      *Options, std::make_unique<FileWriter::CommandListener>(*Options),
      std::make_unique<FileWriter::JobCreator>(),
      createStatusReporter(*Options), UsedRegistrar);
  std::atomic<bool> Running{true};
  std::thread MasterThread([&Master, Logger, &Running] {
    try {
      while (Running) {
        Master.run();
      }
    } catch (std::system_error const &e) {
      Logger->critical(
          "std::system_error  code: {}  category: {}  message: {}  what: {}",
          e.code().value(), e.code().category().name(), e.code().message(),
          e.what());
      throw;
    } catch (std::runtime_error const &e) {
      Logger->critical("std::runtime_error  what: {}", e.what());
      throw;
    } catch (std::exception const &e) {
      Logger->critical("std::exception  what: {}", e.what());
      throw;
    }
  });

  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (GotSignal) {
      Logger->debug("SIGNAL {}", SignalId);
      Running = false;
      GotSignal = false;
      break;
    }
  }

  MasterThread.join();
  Logger->flush();
  return 0;
}
