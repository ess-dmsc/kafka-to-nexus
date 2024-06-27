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

std::unique_ptr<Status::StatusReporter>
createStatusReporter(MainOpt const &MainConfig,
                     std::string const &ApplicationName,
                     std::string const &ApplicationVersion) {
  Kafka::BrokerSettings BrokerSettings =
      MainConfig.StreamerConfiguration.BrokerSettings;
  BrokerSettings.Address = MainConfig.CommandBrokerURI.HostPort;
  auto const StatusInformation =
      Status::ApplicationStatusInfo{MainConfig.StatusMasterInterval,
                                    ApplicationName,
                                    ApplicationVersion,
                                    getHostName(),
                                    MainConfig.ServiceName,
                                    MainConfig.getServiceId(),
                                    getPID()};
  return std::make_unique<Status::StatusReporter>(
      BrokerSettings, MainConfig.CommandBrokerURI.Topic, StatusInformation);
}

bool tryToFindTopics(std::string const &PoolTopic,
                     std::string const &CommandTopic, std::string const &Broker,
                     duration TimeOut,
                     Kafka::BrokerSettings const &BrokerSettings) {
  try {
    auto ListOfTopics =
        Kafka::MetadataEnquirer().getTopicList(Broker, TimeOut, BrokerSettings);
    if (ListOfTopics.find(PoolTopic) == ListOfTopics.end()) {
      auto MsgString = fmt::format(
          R"(Unable to find job pool topic with name "{}".)", PoolTopic);
      LOG_CRITICAL(MsgString);
      throw std::runtime_error(MsgString);
    }
    if (ListOfTopics.find(CommandTopic) == ListOfTopics.end()) {
      auto MsgString = fmt::format(
          R"(Unable to find command topic with name "{}".)", CommandTopic);
      LOG_CRITICAL(MsgString);
      throw std::runtime_error(MsgString);
    }
  } catch (MetadataException const &E) {
    LOG_WARN("Meta data query failed with message: {}", E.what());
    return false;
  }
  return true;
}

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
    fmt::print("{}\n", ApplicationVersion);
    return EXIT_SUCCESS;
  }
  App.clear();

  CLI11_PARSE(App, argc, argv);
  setupLoggerFromOptions(*Options);
  if (not versionOfHDF5IsOk()) {
    LOG_ALERT("Failed HDF5 version check. Exiting.");
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

  auto FQDN = getFQDN();
  std::replace(FQDN.begin(), FQDN.end(), '.', '_');
  std::string metric_prefix = ApplicationName + "." + FQDN;

  std::string prefix_service_component;
  if (Options->ServiceName.empty()) {
    prefix_service_component = Options->getServiceId();
  } else {
    prefix_service_component = Options->ServiceName;
  }
  metric_prefix += "." + prefix_service_component;
  std::unique_ptr<Metrics::IRegistrar> registrar =
      std::make_unique<Metrics::Registrar>(metric_prefix, MetricsReporters);

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
        std::move(registrar));
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
      LOG_CRITICAL(
          "std::system_error  code: {}  category: {}  message: {}  what: {}",
          e.code().value(), e.code().category().name(), e.code().message(),
          e.what());
      break;
    } catch (std::runtime_error const &e) {
      LOG_CRITICAL("std::runtime_error  what: {}", e.what());
      break;
    } catch (std::exception const &e) {
      LOG_CRITICAL("std::exception  what: {}", e.what());
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
