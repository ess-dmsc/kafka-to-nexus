// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "kafka-to-nexus.h"
#include "CLIOptions.h"
#include "FlatbufferReader.h"
#include "HDFVersionCheck.h"
#include "JobCreator.h"
#include "Metrics/CarbonSink.h"
#include "Metrics/LogSink.h"
#include "Metrics/Registrar.h"
#include "Metrics/Reporter.h"
#include "Status/StatusInfo.h"
#include "Version.h"
#include "WriterRegistrar.h"
#include <CLI/CLI.hpp>

// These should only be visible in this translation unit
static std::atomic<RunStates> RunState{RunStates::Running};

int main(int argc, char **argv) {
  std::string const ApplicationName = "kafka-to-nexus";
  std::string const ApplicationVersion = GetVersion();
  CLI::App App{fmt::format(
      "{} {:.7} (ESS, BrightnESS)\n"
      "https://github.com/ess-dmsc/kafka-to-nexus\n\n"
      "Writes NeXus files in a format specified with a json template.\n"
      "Writer modules can be used to populate the file from Kafka topics.\n",
      ApplicationName, ApplicationVersion)};
  auto Options = std::make_unique<MainOpt>();
  setCLIOptions(App, *Options);

  try {
    App.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    // Do nothing, we only care about the version flag in this first pass.
  }

  if (Options->PrintVersion) {
    fmt::print("{}\n", GetVersion());
    return EXIT_SUCCESS;
  }
  App.clear();

  CLI11_PARSE(App, argc, argv);
  setupLoggerFromOptions(*Options);
  if (not versionOfHDF5IsOk()) {
    LOG_ERROR("Failed HDF5 version check. Exiting.");
    return EXIT_FAILURE;
  }

  if (Options->ListWriterModules) {
    fmt::print("\n-- Known flatbuffer metadata extractors\n");
    for (auto &ReaderPair :
         FileWriter::FlatbufferReaderRegistry::getReaders()) {
      fmt::print("---- {}\n", ReaderPair.first);
    }
    fmt::print("\n-- Known writer modules\n");
    for (auto &WriterPair : WriterModule::Registry::getFactoryIdsAndNames()) {
      fmt::print("---- {} : {}\n", WriterPair.Id, WriterPair.Name);
    }
    return EXIT_SUCCESS;
  }
  using std::chrono_literals::operator""ms;
  std::vector<std::shared_ptr<Metrics::Reporter>> MetricsReporters;
  MetricsReporters.push_back(std::make_shared<Metrics::Reporter>(
      std::make_unique<Metrics::LogSink>(), 60s));

  if (not Options->GrafanaCarbonAddress.HostPort.empty()) {
    auto HostName = Options->GrafanaCarbonAddress.Host;
    auto Port = Options->GrafanaCarbonAddress.Port;
    MetricsReporters.push_back(std::make_shared<Metrics::Reporter>(
        std::make_unique<Metrics::CarbonSink>(HostName, Port), 10s));
  }

  Metrics::Registrar MainRegistrar(ApplicationName, MetricsReporters);
  auto FQDN = getFQDN();
  std::replace(FQDN.begin(), FQDN.end(), '.', '_');
  auto UsedRegistrar = MainRegistrar.getNewRegistrar(FQDN);
  if (Options->ServiceName.empty()) {
    UsedRegistrar = UsedRegistrar.getNewRegistrar(Options->getServiceId());
  } else {
    UsedRegistrar = UsedRegistrar.getNewRegistrar(Options->ServiceName);
  }

  std::signal(SIGHUP, [](int signal) { signal_handler(signal, RunState); });
  std::signal(SIGINT, [](int signal) { signal_handler(signal, RunState); });
  std::signal(SIGTERM, [](int signal) { signal_handler(signal, RunState); });

  std::unique_ptr<FileWriter::Master> MasterPtr;

  auto GenerateMaster = [&]() {
    return std::make_unique<FileWriter::Master>(
        *Options,
        std::make_unique<Command::Handler>(
            Options->getServiceId(),
            Options->StreamerConfiguration.BrokerSettings, Options->JobPoolURI,
            Options->CommandBrokerURI),
        createStatusReporter(*Options, ApplicationName, ApplicationVersion),
        UsedRegistrar);
  };

  Status::StatusService status(Options->ServerStatusPort);
  status.startThread();

  bool FindTopicMode{true};
  duration CMetaDataTimeout{
      Options->StreamerConfiguration.BrokerSettings.MinMetadataTimeout};
  auto PoolTopic = Options->JobPoolURI.Topic;
  auto CommandTopic = Options->CommandBrokerURI.Topic;
  LOG_DEBUG("Starting run loop.");
  LOG_DEBUG("Retrieving topic names from broker.");
  while (!shouldStop(MasterPtr, FindTopicMode, RunState)) {
    try {
      if (FindTopicMode) {
        if (tryToFindTopics(PoolTopic, CommandTopic,
                            Options->CommandBrokerURI.HostPort,
                            CMetaDataTimeout,
                            Options->StreamerConfiguration.BrokerSettings)) {
          LOG_DEBUG("Command and status topics found, starting master.");
          MasterPtr = GenerateMaster();
          FindTopicMode = false;
        } else {
          CMetaDataTimeout *= 2;
          if (CMetaDataTimeout > Options->StreamerConfiguration.BrokerSettings
                                     .MaxMetadataTimeout) {
            CMetaDataTimeout = Options->StreamerConfiguration.BrokerSettings
                                   .MaxMetadataTimeout;
          }
          LOG_WARN(
              R"(Meta data call for retrieving the command topic ("{}") from the broker failed. Re-trying with a timeout of {} ms.)",
              CommandTopic,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  CMetaDataTimeout)
                  .count());
        }
      } else {
        MasterPtr->run();
      }
    } catch (std::system_error const &e) {
      LOG_ERROR(
          "std::system_error  code: {}  category: {}  message: {}  what: {}",
          e.code().value(), e.code().category().name(), e.code().message(),
          e.what());
      break;
    } catch (std::runtime_error const &e) {
      LOG_ERROR("std::runtime_error  what: {}", e.what());
      break;
    } catch (std::exception const &e) {
      LOG_ERROR("std::exception  what: {}", e.what());
      break;
    }
  }
  if (RunState == RunStates::SIGINT_KafkaWait) {
    LOG_DEBUG("Giving a grace period to Kafka.");
    std::this_thread::sleep_for(3s);
  }
  LOG_INFO("Exiting.");
  Log::Flush();
  return EXIT_SUCCESS;
}
