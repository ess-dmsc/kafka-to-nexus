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
#include "WriterModule/mdat/mdat_Writer.h"
#include "WriterModuleBase.h"
#include "WriterRegistrar.h"
#include "json.h"
#include <algorithm>

using std::vector;

namespace FileWriter {

using nlohmann::json;

std::vector<ModuleHDFInfo>
initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString,
              std::filesystem::path const &template_path,
              bool const &is_legacy_writing) {
  try {
    json const NexusStructure = json::parse(NexusStructureString);
    std::vector<ModuleHDFInfo> ModuleHDFInfoList;
    Task.InitialiseHdf(NexusStructure, ModuleHDFInfoList, template_path,
                       is_legacy_writing);
    return ModuleHDFInfoList;
  } catch (nlohmann::detail::exception const &Error) {
    throw std::runtime_error(
        fmt::format("Could not parse NeXus structure JSON. The error was: {}'",
                    Error.what()));
  }
}

ModuleSettings
extractModuleInformationFromJsonForSource(ModuleHDFInfo const &ModuleInfo) {
  if (ModuleInfo.WriterModule.empty()) {
    throw std::runtime_error("Empty writer module name encountered.");
  }
  ModuleSettings ModuleSettings;
  ModuleSettings.ModuleHDFInfoObj = ModuleInfo;

  json ConfigStream = json::parse(ModuleSettings.ModuleHDFInfoObj.ConfigStream);

  ModuleSettings.ConfigStreamJson = ConfigStream.dump();
  ModuleSettings.Source =
      Command::Parser::getRequiredValue<std::string>("source", ConfigStream);
  ModuleSettings.Module = ModuleInfo.WriterModule;
  ModuleSettings.isLink = false;

  if (ModuleSettings.Module == "link") {
    ModuleSettings.Name =
        Command::Parser::getRequiredValue<std::string>("name", ConfigStream);
    ModuleSettings.isLink = true;
  } else {
    ModuleSettings.Topic =
        Command::Parser::getRequiredValue<std::string>("topic", ConfigStream);
  }

  auto Attributes =
      Command::Parser::getOptionalValue<json>("attributes", ConfigStream, "")
          .dump();
  if (!Attributes.empty() && Attributes != "\"\"") {
    Logger::Info(
        "Writing of writer module attributes to parent group has been "
        "removed. Attributes should be assigned directly to group. The "
        "(unused) attributes belongs to dataset with source name \"{}\" "
        "from topic \"{}\".",
        ModuleSettings.Source, ModuleSettings.Topic);
  }

  return ModuleSettings;
}

/// Helper to extract information about the provided links and streams.
std::vector<ModuleSettings> extractModuleInformationFromJson(
    std::vector<ModuleHDFInfo> const &ModuleHDFInfoList) {
  std::vector<ModuleSettings> SettingsList;
  for (auto const &ModuleHDFInfo : ModuleHDFInfoList) {
    try {
      SettingsList.push_back(
          extractModuleInformationFromJsonForSource(ModuleHDFInfo));
    } catch (json::parse_error const &E) {
      Logger::Info(
          "Invalid module configuration JSON encountered. The error was: {}",
          E.what());
      continue;
    } catch (std::runtime_error const &E) {
      Logger::Info("Unknown exception encountered when extracting module "
                   "information. The error was: {}",
                   E.what());
      continue;
    }
  }
  Logger::Info("Command contains {} links and streams.", SettingsList.size());
  return SettingsList;
}

std::vector<ModuleHDFInfo>
extractMdatModules(std::vector<ModuleHDFInfo> &Modules) {
  auto it = std::stable_partition(Modules.begin(), Modules.end(),
                                  [](ModuleHDFInfo const &Module) {
                                    return Module.WriterModule != "mdat";
                                  });

  std::vector<ModuleHDFInfo> mdatInfoList{it, Modules.end()};
  Modules.erase(it, Modules.end());
  return mdatInfoList;
};

std::unique_ptr<StreamController> createFileWritingJob(
    Command::StartMessage const &StartInfo, StreamerOptions const &Settings,
    std::filesystem::path const &filepath, Metrics::IRegistrar *Registrar,
    MetaData::TrackerPtr const &Tracker,
    std::filesystem::path const &template_path,
    std::shared_ptr<Kafka::MetadataEnquirer> metadata_enquirer,
    std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory) {
  auto Task = std::make_unique<FileWriterTask>(StartInfo.JobID, filepath,
                                               Registrar, Tracker);

  bool const is_legacy_writing = StartInfo.InstrumentName.empty();

  std::vector<ModuleHDFInfo> ModuleHDFInfoList = initializeHDF(
      *Task, StartInfo.NexusStructure, template_path, is_legacy_writing);
  std::vector<ModuleHDFInfo> mdatInfoList =
      extractMdatModules(ModuleHDFInfoList);

  auto mdatWriter = std::make_unique<WriterModule::mdat::mdat_Writer>();
  mdatWriter->define_metadata(mdatInfoList);

  std::vector<ModuleSettings> SettingsList =
      extractModuleInformationFromJson(ModuleHDFInfoList);
  std::vector<ModuleSettings> StreamSettingsList;
  std::vector<ModuleSettings> LinkSettingsList;

  for (auto &Item : SettingsList) {
    if (Item.isLink) {
      LinkSettingsList.push_back(std::move(Item));
    } else {
      StreamSettingsList.push_back(std::move(Item));
    }
  }

  for (size_t i = 0; i < StreamSettingsList.size(); ++i) {
    auto &Item = StreamSettingsList[i];
    auto StreamGroup = hdf5::node::get_group(
        Task->hdfGroup(), Item.ModuleHDFInfoObj.HDFParentName);
    std::vector<ModuleSettings> TemporaryStreamSettings;
    try {
      Item.WriterModule = generateWriterInstance(Item);
      for (auto const &ExtraModule :
           Item.WriterModule->getEnabledExtraModules()) {
        auto ItemCopy = Item.getCopyForExtraModule();
        ItemCopy.Module = ExtraModule;
        TemporaryStreamSettings.push_back(std::move(ItemCopy));
      }
      setWriterHDFAttributes(StreamGroup, Item);
      Item.WriterModule->init_hdf(StreamGroup);
    } catch (std::runtime_error const &E) {
      auto ErrorMsg = fmt::format(
          R"(Could not initialise stream at path "{}" with configuration JSON "{}". Error was: {})",
          Item.ModuleHDFInfoObj.HDFParentName,
          Item.ModuleHDFInfoObj.ConfigStream, E.what());
      throw std::runtime_error(ErrorMsg);
    }
    try {
      Item.WriterModule->register_meta_data(StreamGroup, Tracker);
    } catch (std::exception const &E) {
      throw std::runtime_error(fmt::format(
          R"(Exception encountered in WriterModule::Base::register_meta_data(). Module: "{}" Source: "{}"  Error message: {})",
          Item.Module, Item.Source, E.what()));
    }
    std::transform(TemporaryStreamSettings.begin(),
                   TemporaryStreamSettings.end(),
                   std::back_inserter(StreamSettingsList),
                   [](auto &Element) { return std::move(Element); });
  }
  Task->writeLinks(LinkSettingsList);
  Task->switchToWriteMode();

  addStreamSourceToWriterModule(StreamSettingsList, *Task);

  Logger::Info("Write file with job_id: {}", Task->jobID());

  return std::make_unique<StreamController>(
      std::move(Task), std::move(mdatWriter), Settings, Registrar, Tracker,
      metadata_enquirer, consumer_factory);
}

void createFileWriterTemplate(Command::StartMessage const &StartInfo,
                              std::filesystem::path const &filepath,
                              Metrics::IRegistrar *Registrar,
                              MetaData::TrackerPtr const &Tracker) {
  auto Task = std::make_unique<FileWriterTask>(StartInfo.JobID, filepath,
                                               Registrar, Tracker);
  std::filesystem::path const template_path;
  bool const is_legacy_writing = false;
  std::vector<ModuleHDFInfo> ModuleHDFInfoList = initializeHDF(
      *Task, StartInfo.NexusStructure, template_path, is_legacy_writing);
}

void addStreamSourceToWriterModule(vector<ModuleSettings> &StreamSettingsList,
                                   FileWriterTask &Task) {
  for (auto &StreamSettings : StreamSettingsList) {
    try {
      try {
        auto RootGroup = Task.hdfGroup();
        auto StreamGroup = hdf5::node::get_group(
            RootGroup, StreamSettings.ModuleHDFInfoObj.HDFParentName);
        auto Err = StreamSettings.WriterModule->reopen({StreamGroup});
        if (Err != WriterModule::InitResult::OK) {
          Logger::Error("Failed when reopening HDF datasets for stream {}",
                        StreamSettings.ModuleHDFInfoObj.HDFParentName);
          continue;
        }
      } catch (std::runtime_error const &e) {
        Logger::Error("Exception on WriterModule::Base->reopen(): {}",
                      e.what());
        continue;
      }

      // Create a Source instance for the stream and add to the task.
      auto FoundModule = WriterModule::Registry::find(StreamSettings.Module);
      Source ThisSource(StreamSettings.Source, FoundModule.second.Id,
                        FoundModule.second.Name, StreamSettings.Topic,
                        std::move(StreamSettings.WriterModule));
      Task.addSource(std::move(ThisSource));
    } catch (std::runtime_error const &E) {
      Logger::Info(
          "Exception while initializing writer module {} for source {}: {}",
          StreamSettings.Module, StreamSettings.Source, E.what());
      continue;
    }
  }
}

std::unique_ptr<WriterModule::Base>
generateWriterInstance(ModuleSettings const &StreamInfo) {
  WriterModule::Registry::FactoryAndID ModuleFactory;
  try {
    ModuleFactory = WriterModule::Registry::find(StreamInfo.Module);
  } catch (std::exception const &E) {
    throw std::runtime_error(fmt::format(
        R"(Error while getting module with name/id "{}" for source "{}". Message was: {})",
        StreamInfo.Module, StreamInfo.Source, E.what()));
  }

  auto HDFWriterModule = ModuleFactory.first();
  if (!HDFWriterModule) {
    throw std::runtime_error(fmt::format(
        "Can not instantiate a writer module for module name/id '{}'",
        StreamInfo.Module));
  }

  try {
    HDFWriterModule->parse_config(StreamInfo.ConfigStreamJson);
  } catch (std::exception const &E) {
    std::runtime_error(fmt::format(
        R"(Exception encountered in WriterModule::Base::parse_config()  Module: "{}" Source: "{}"  Error message: {})",
        StreamInfo.Module, StreamInfo.Source, E.what()));
  }
  return HDFWriterModule;
}

void setWriterHDFAttributes(hdf5::node::Group &RootNode,
                            ModuleSettings const &StreamInfo) {
  auto StreamGroup = hdf5::node::get_group(
      RootNode, StreamInfo.ModuleHDFInfoObj.HDFParentName);

  auto writeAttributesList = [&StreamGroup,
                              &StreamInfo](std::vector<
                                           std::pair<std::string, std::string>>
                                               Attributes) {
    for (auto const &Attribute : Attributes) {
      if (StreamGroup.attributes.exists(Attribute.first)) {
        Logger::Debug(
            R"(Will not write attribute with key "{}" at "{}" as it already exists.)",
            Attribute.first, StreamInfo.ModuleHDFInfoObj.HDFParentName);
        continue;
      }
      auto HdfAttribute =
          StreamGroup.attributes.create<std::string>(Attribute.first);
      HdfAttribute.write(Attribute.second);
    }
  };
  writeAttributesList({
      {"NX_class", std::string(StreamInfo.WriterModule->defaultNeXusClass())},
      {"topic", StreamInfo.Topic},
      {"source", StreamInfo.Source},
      {"writer_module", StreamInfo.Module},
  });
  if (!StreamInfo.Attributes.empty()) {
    Logger::Info(
        "Writing of writer module attributes to parent group has been "
        "removed. Attributes should be assigned directly to group. The "
        "(unused) attributes belongs to dataset with source name \"{}\" "
        "on topic \"{}\".",
        StreamInfo.Source, StreamInfo.Topic);
  }
}

} // namespace FileWriter
