// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file  CommandHandler.cpp

#include "JobCreator.h"
#include "CommandSystem/Parser.h"
#include "FileWriterTask.h"
#include "HDFOperations.h"
#include "Msg.h"
#include "StreamController.h"
#include "WriterModuleBase.h"
#include "WriterRegistrar.h"
#include "json.h"
#include <algorithm>

using std::vector;

namespace FileWriter {

using nlohmann::json;

std::vector<StreamHDFInfo>
JobCreator::initializeHDF(FileWriterTask &Task,
                          std::string const &NexusStructureString) {
  try {
    json const NexusStructure = json::parse(NexusStructureString);
    std::vector<StreamHDFInfo> StreamHDFInfoList;
    Task.InitialiseHdf(NexusStructure.dump(), StreamHDFInfoList);
    return StreamHDFInfoList;
  } catch (nlohmann::detail::exception const &Error) {
    throw std::runtime_error(
        fmt::format("Could not parse NeXus structure JSON '{}'", Error.what()));
  }
}

StreamSettings
extractStreamInformationFromJsonForSource(StreamHDFInfo const &StreamInfo) {
  StreamSettings StreamSettings;
  StreamSettings.StreamHDFInfoObj = StreamInfo;

  json ConfigStream = json::parse(StreamSettings.StreamHDFInfoObj.ConfigStream);

  auto ConfigStreamInner =
      Command::Parser::getRequiredValue<json>("stream", ConfigStream);
  StreamSettings.ConfigStreamJson = ConfigStreamInner.dump();
  StreamSettings.Topic = Command::Parser::getRequiredValue<std::string>(
      "topic", ConfigStreamInner);
  StreamSettings.Source = Command::Parser::getRequiredValue<std::string>(
      "source", ConfigStreamInner);
  StreamSettings.Module = Command::Parser::getRequiredValue<std::string>(
      "writer_module", ConfigStreamInner);
  StreamSettings.Attributes =
      Command::Parser::getOptionalValue<json>("attributes", ConfigStream, "")
          .dump();

  return StreamSettings;
}

void setUpHdfStructure(StreamSettings const &StreamSettings,
                       std::unique_ptr<FileWriterTask> const &Task) {
  WriterModule::Registry::FactoryAndID ModuleFactory;
  try {
    ModuleFactory = WriterModule::Registry::find(StreamSettings.Module);
  } catch (std::exception const &E) {
    throw std::runtime_error(
        fmt::format("Error while getting module with name \"{}\" for source "
                    "\"{}\". Message was: {}",
                    StreamSettings.Module, StreamSettings.Source, E.what()));
  }

  auto HDFWriterModule = ModuleFactory.first();
  if (!HDFWriterModule) {
    throw std::runtime_error(
        fmt::format("Can not instantiate a writer module for module name '{}'",
                    StreamSettings.Module));
  }

  auto RootGroup = Task->hdfGroup();
  try {
    HDFWriterModule->parse_config(StreamSettings.ConfigStreamJson);
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Exception while WriterModule::Base::parse_config  module: {} "
        " source: {}  error message: {}",
        StreamSettings.Module, StreamSettings.Source, E.what())));
  }

  auto StreamGroup = hdf5::node::get_group(
      RootGroup, StreamSettings.StreamHDFInfoObj.HDFParentName);

  auto writeAttributesList =
      [&StreamGroup, &StreamSettings](
          std::vector<std::pair<std::string, std::string>> Attributes) {
        for (auto Attribute : Attributes) {
          if (StreamGroup.attributes.exists(Attribute.first)) {
            StreamGroup.attributes.remove(Attribute.first);
            LOG_DEBUG(
                "Replacing (existing) attribute with key \"{}\" at \"{}\".",
                Attribute.first, StreamSettings.StreamHDFInfoObj.HDFParentName);
          }
          auto HdfAttribute =
              StreamGroup.attributes.create<std::string>(Attribute.first);
          HdfAttribute.write(Attribute.second);
        }
      };
  writeAttributesList(
      {{"NX_class", std::string(HDFWriterModule->defaultNeXusClass())},
       {"topic", StreamSettings.Topic},
       {"source", StreamSettings.Source}});

  auto AttributesJson = nlohmann::json::parse(StreamSettings.Attributes);
  HDFOperations::writeAttributes(StreamGroup, &AttributesJson, SharedLogger());

  HDFWriterModule->init_hdf({StreamGroup});
}

/// Helper to extract information about the provided streams.
/// \param Logger Pointer to spdlog instance to be used for logging.
static vector<StreamSettings>
extractStreamInformationFromJson(std::unique_ptr<FileWriterTask> const &Task,
                                 std::vector<StreamHDFInfo> &StreamHDFInfoList,
                                 SharedLogger const &Logger) {
  Logger->info("Command contains {} streams", StreamHDFInfoList.size());
  std::vector<StreamSettings> StreamSettingsList;
  for (auto &StreamHDFInfo : StreamHDFInfoList) {
    try {
      StreamSettingsList.push_back(
          extractStreamInformationFromJsonForSource(StreamHDFInfo));
      Logger->info("Adding stream: {}",
                   StreamSettingsList.back().ConfigStreamJson);
      setUpHdfStructure(StreamSettingsList.back(), Task);
      StreamHDFInfo.InitialisedOk = true;
    } catch (json::parse_error const &E) {
      Logger->warn("Invalid json: {}", StreamHDFInfo.ConfigStream);
      continue;
    } catch (std::runtime_error const &E) {
      Logger->warn("Exception while initialising writer module  what: {}  "
                   "parent: {}  json: {}",
                   E.what(), StreamHDFInfo.HDFParentName,
                   StreamHDFInfo.ConfigStream);
      continue;
    } catch (...) {
      Logger->error("Unknown error caught while trying to initialise stream  "
                    "parent: {}  json: {}",
                    StreamHDFInfo.HDFParentName, StreamHDFInfo.ConfigStream);
    }
  }
  return StreamSettingsList;
}

std::unique_ptr<IStreamController>
JobCreator::createFileWritingJob(Command::StartInfo const &StartInfo,
                                 MainOpt &Settings, SharedLogger const &Logger,
                                 Metrics::Registrar Registrar) {
  auto Task = std::make_unique<FileWriterTask>();
  Task->setJobId(StartInfo.JobID);
  Task->setFilename(Settings.HDFOutputPrefix, StartInfo.Filename);

  std::vector<StreamHDFInfo> StreamHDFInfoList =
      initializeHDF(*Task, StartInfo.NexusStructure);

  std::vector<StreamSettings> StreamSettingsList =
      extractStreamInformationFromJson(Task, StreamHDFInfoList, Logger);

  if (Settings.AbortOnUninitialisedStream) {
    for (auto const &Item : StreamHDFInfoList) {
      // cppcheck-suppress useStlAlgorithm
      if (!Item.InitialisedOk) {
        throw std::runtime_error(fmt::format("Could not initialise {}  {}",
                                             Item.HDFParentName,
                                             Item.ConfigStream));
      }
    }
  }

  addStreamSourceToWriterModule(StreamSettingsList, Task);

  Settings.StreamerConfiguration.StartTimestamp = StartInfo.StartTime;
  Settings.StreamerConfiguration.StopTimestamp = time_point(StartInfo.StopTime);
  Settings.StreamerConfiguration.BrokerSettings.Address =
      StartInfo.BrokerInfo.HostPort;

  Logger->info("Write file with job_id: {}", Task->jobID());
  return std::make_unique<StreamController>(
      std::move(Task), Settings.StreamerConfiguration, Registrar);
}

void JobCreator::addStreamSourceToWriterModule(
    vector<StreamSettings> const &StreamSettingsList,
    std::unique_ptr<FileWriterTask> &Task) {
  auto Logger = getLogger();

  for (auto const &StreamSettings : StreamSettingsList) {
    Logger->trace("Add Source: {}", StreamSettings.Topic);
    WriterModule::Registry::FactoryAndID ModuleFactory;

    try {
      ModuleFactory = WriterModule::Registry::find(StreamSettings.Module);
    } catch (std::exception const &E) {
      Logger->info("WriterModule '{}' is not available, error {}",
                   StreamSettings.Module, E.what());
      continue;
    }

    auto AcceptedFlatbufferID = ModuleFactory.second;
    auto HDFWriterModule = ModuleFactory.first();
    if (!HDFWriterModule) {
      Logger->info("Can not create a writer module for '{}'",
                   StreamSettings.Module);
      continue;
    }

    try {
      // Reopen the previously created HDF dataset.
      HDFWriterModule->parse_config(StreamSettings.ConfigStreamJson);
      try {
        auto RootGroup = Task->hdfGroup();
        auto StreamGroup = hdf5::node::get_group(
            RootGroup, StreamSettings.StreamHDFInfoObj.HDFParentName);
        auto Err = HDFWriterModule->reopen({StreamGroup});
        if (Err != WriterModule::InitResult::OK) {
          Logger->error("can not reopen HDF file for stream {}",
                        StreamSettings.StreamHDFInfoObj.HDFParentName);
          continue;
        }
      } catch (std::runtime_error const &e) {
        Logger->error("Exception on WriterModule::Base->reopen(): {}",
                      e.what());
        continue;
      }

      // Create a Source instance for the stream and add to the task.
      Source ThisSource(StreamSettings.Source, AcceptedFlatbufferID,
                        StreamSettings.Module, StreamSettings.Topic,
                        move(HDFWriterModule));
      Task->addSource(std::move(ThisSource));
    } catch (std::runtime_error const &E) {
      Logger->warn(
          "Exception while initializing writer module {} for source {}: {}",
          StreamSettings.Module, StreamSettings.Source, E.what());
      continue;
    }
  }
}
} // namespace FileWriter
