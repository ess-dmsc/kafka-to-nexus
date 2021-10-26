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
initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString) {
  try {
    json const NexusStructure = json::parse(NexusStructureString);
    std::vector<StreamHDFInfo> StreamHDFInfoList;
    Task.InitialiseHdf(NexusStructure.dump(), StreamHDFInfoList);
    return StreamHDFInfoList;
  } catch (nlohmann::detail::exception const &Error) {
    throw std::runtime_error(
        fmt::format("Could not parse NeXus structure JSON. The error was: {}'",
                    Error.what()));
  }
}

StreamSettings
extractStreamInformationFromJsonForSource(StreamHDFInfo const &StreamInfo) {
  if (StreamInfo.WriterModule.empty()) {
    throw std::runtime_error("Empty writer module name encountered.");
  }
  StreamSettings StreamSettings;
  StreamSettings.StreamHDFInfoObj = StreamInfo;

  json ConfigStream = json::parse(StreamSettings.StreamHDFInfoObj.ConfigStream);

  StreamSettings.ConfigStreamJson = ConfigStream.dump();
  StreamSettings.Topic =
      Command::Parser::getRequiredValue<std::string>("topic", ConfigStream);
  StreamSettings.Source =
      Command::Parser::getRequiredValue<std::string>("source", ConfigStream);
  StreamSettings.Module = StreamInfo.WriterModule;
  StreamSettings.Attributes =
      Command::Parser::getOptionalValue<json>("attributes", ConfigStream, "")
          .dump();

  return StreamSettings;
}

LinkSettings extractLinkInformationFromJsonForLinking(StreamHDFInfo const &LinkInfo){
  LinkSettings LinkSettings;
  LinkSettings.Name = LinkInfo.WriterModule;
  LinkSettings.Target = LinkInfo.ConfigStream;
  LinkSettings.Path = LinkInfo.HDFParentName;
  
  return LinkSettings;
}

/// Helper to extract information about the provided stream.
static std::vector<StreamSettings> extractStreamInformationFromJson(
  std::vector<StreamHDFInfo> &StreamHDFInfoList){
  std::vector<StreamSettings> StreamSettingsList;
  for (auto &StreamHDFInfo : StreamHDFInfoList) {
    try {
      if(not StreamHDFInfo.isLink){
        StreamSettingsList.push_back(
            extractStreamInformationFromJsonForSource(StreamHDFInfo));
      }
    } catch (json::parse_error const &E) {
      LOG_WARN(
          "Invalid stream configuration JSON encountered. The error was: {}",
          E.what());
      continue;
    } catch (std::runtime_error const &E) {
      LOG_WARN("Unknown exception encountered when extracting stream "
               "information. The error was: {}",
               E.what());
      continue;
    }
  }
  LOG_INFO("Command contains {} streams", StreamSettingsList.size());
  return StreamSettingsList;
}


/// Helper to extract information about the provided links.
static std::vector<LinkSettings> extractLinkInformationFromJson(
    std::vector<StreamHDFInfo> &StreamHDFInfoList) {
  std::vector<LinkSettings> LinkSettingsList;
  for (auto &StreamHDFInfo : StreamHDFInfoList) {
    try {
      if(StreamHDFInfo.isLink){
        LinkSettingsList.push_back(
            extractLinkInformationFromJsonForLinking(StreamHDFInfo));
      }
    } catch (json::parse_error const &E) {
      LOG_WARN(
          "Invalid link configuration JSON encountered. The error was: {}",
          E.what());
      continue;
    } catch (std::runtime_error const &E) {
      LOG_WARN("Unknown exception encountered when extracting link "
               "information. The error was: {}",
               E.what());
      continue;
    }
  }
  LOG_INFO("Command contains {} links", LinkSettingsList.size());
  return LinkSettingsList;
}

std::unique_ptr<IStreamController>
createFileWritingJob(Command::StartInfo const &StartInfo, MainOpt &Settings,
                     Metrics::Registrar Registrar,
                     MetaData::TrackerPtr const &Tracker) {
  auto Task = std::make_unique<FileWriterTask>(Tracker);
  Task->setJobId(StartInfo.JobID);
  Task->setFilename(Settings.HDFOutputPrefix, StartInfo.Filename);

  std::vector<StreamHDFInfo> StreamHDFInfoList =
      initializeHDF(*Task, StartInfo.NexusStructure);

  auto StreamSettingsList = extractStreamInformationFromJson(StreamHDFInfoList);
  auto LinkSettingsList = extractLinkInformationFromJson(StreamHDFInfoList);

  for (auto &Item : StreamSettingsList) {
    auto StreamGroup = hdf5::node::get_group(
        Task->hdfGroup(), Item.StreamHDFInfoObj.HDFParentName);
    try {
      Item.WriterModule = generateWriterInstance(Item);

      setWriterHDFAttributes(StreamGroup, Item);
      Item.WriterModule->init_hdf(StreamGroup);
    } catch (std::runtime_error const &E) {
      auto ErrorMsg = fmt::format("Could not initialise stream at path \"{}\" "
                                  "with configuration JSON \"{}\".",
                                  Item.StreamHDFInfoObj.HDFParentName,
                                  Item.StreamHDFInfoObj.ConfigStream);
      if (Settings.AbortOnUninitialisedStream) {
        std::throw_with_nested(std::runtime_error(ErrorMsg));
      }
    }
    try {
      Item.WriterModule->register_meta_data(StreamGroup, Tracker);
    } catch (std::exception const &E) {
      std::throw_with_nested(std::runtime_error(fmt::format(
          "Exception encountered in WriterModule::Base::register_meta_data(). "
          "Module: \"{}\" "
          " Source: \"{}\"  Error message: {}",
          Item.Module, Item.Source, E.what())));
    }
  }
  Task->writeLinks(LinkSettingsList);
  Task->switchToWriteMode();

  addStreamSourceToWriterModule(StreamSettingsList, Task);

  Settings.StreamerConfiguration.StartTimestamp = StartInfo.StartTime;
  Settings.StreamerConfiguration.StopTimestamp = StartInfo.StopTime;
  Settings.StreamerConfiguration.BrokerSettings.Address =
      StartInfo.BrokerInfo.HostPort;

  LOG_INFO("Write file with job_id: {}", Task->jobID());
  return std::make_unique<StreamController>(
      std::move(Task), Settings.StreamerConfiguration, Registrar, Tracker);
}

void addStreamSourceToWriterModule(vector<StreamSettings> &StreamSettingsList,
                                   std::unique_ptr<FileWriterTask> &Task) {

  for (auto &StreamSettings : StreamSettingsList) {
    try {
      try {
        auto RootGroup = Task->hdfGroup();
        auto StreamGroup = hdf5::node::get_group(
            RootGroup, StreamSettings.StreamHDFInfoObj.HDFParentName);
        auto Err = StreamSettings.WriterModule->reopen({StreamGroup});
        if (Err != WriterModule::InitResult::OK) {
          LOG_ERROR("Failed when reopening HDF datasets for stream {}",
                    StreamSettings.StreamHDFInfoObj.HDFParentName);
          continue;
        }
      } catch (std::runtime_error const &e) {
        LOG_ERROR("Exception on WriterModule::Base->reopen(): {}", e.what());
        continue;
      }

      // Create a Source instance for the stream and add to the task.
      Source ThisSource(
          StreamSettings.Source,
          WriterModule::Registry::find(StreamSettings.Module).second,
          StreamSettings.Module, StreamSettings.Topic,
          move(StreamSettings.WriterModule));
      Task->addSource(std::move(ThisSource));
    } catch (std::runtime_error const &E) {
      LOG_WARN(
          "Exception while initializing writer module {} for source {}: {}",
          StreamSettings.Module, StreamSettings.Source, E.what());
      continue;
    }
  }
}

std::unique_ptr<WriterModule::Base>
generateWriterInstance(StreamSettings const &StreamInfo) {
  WriterModule::Registry::FactoryAndID ModuleFactory;
  try {
    ModuleFactory = WriterModule::Registry::find(StreamInfo.Module);
  } catch (std::exception const &E) {
    throw std::runtime_error(
        fmt::format("Error while getting module with name \"{}\" for source "
                    "\"{}\". Message was: {}",
                    StreamInfo.Module, StreamInfo.Source, E.what()));
  }

  auto HDFWriterModule = ModuleFactory.first();
  if (!HDFWriterModule) {
    throw std::runtime_error(
        fmt::format("Can not instantiate a writer module for module name '{}'",
                    StreamInfo.Module));
  }

  try {
    HDFWriterModule->parse_config(StreamInfo.ConfigStreamJson);
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Exception encountered in "
                    "WriterModule::Base::parse_config()  Module: \"{}\" "
                    " Source: \"{}\"  Error message: {}",
                    StreamInfo.Module, StreamInfo.Source, E.what())));
  }
  return HDFWriterModule;
}

void setWriterHDFAttributes(hdf5::node::Group &RootNode,
                            StreamSettings const &StreamInfo) {
  auto StreamGroup = hdf5::node::get_group(
      RootNode, StreamInfo.StreamHDFInfoObj.HDFParentName);

  auto writeAttributesList =
      [&StreamGroup, &StreamInfo](
          std::vector<std::pair<std::string, std::string>> Attributes) {
        for (auto Attribute : Attributes) {
          if (StreamGroup.attributes.exists(Attribute.first)) {
            StreamGroup.attributes.remove(Attribute.first);
            LOG_DEBUG(
                "Replacing (existing) attribute with key \"{}\" at \"{}\".",
                Attribute.first, StreamInfo.StreamHDFInfoObj.HDFParentName);
          }
          auto HdfAttribute =
              StreamGroup.attributes.create<std::string>(Attribute.first);
          HdfAttribute.write(Attribute.second);
        }
      };
  writeAttributesList(
      {{"NX_class", std::string(StreamInfo.WriterModule->defaultNeXusClass())},
       {"topic", StreamInfo.Topic},
       {"source", StreamInfo.Source}});

  auto AttributesJson = nlohmann::json::parse(StreamInfo.Attributes);
  HDFOperations::writeAttributes(StreamGroup, AttributesJson);
}

} // namespace FileWriter
