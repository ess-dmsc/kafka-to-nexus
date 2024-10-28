// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "CommandSystem/Commands.h"
#include "FileWriterTask.h"
#include "MainOpt.h"
#include "Metrics/Registrar.h"
#include "ModuleSettings.h"
#include "StreamController.h"
#include "json.h"
#include <memory>

namespace FileWriter {

std::unique_ptr<StreamController> createFileWritingJob(
    Command::StartMessage const &StartInfo, StreamerOptions const &Settings,
    std::filesystem::path const &filepath, Metrics::IRegistrar *Registrar,
    MetaData::TrackerPtr const &Tracker,
    std::filesystem::path const &template_path,
    std::shared_ptr<Kafka::MetadataEnquirer> metadata_enquirer =
        std::make_shared<Kafka::MetadataEnquirer>(),
    std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory =
        std::make_shared<Kafka::ConsumerFactory>());

void createFileWriterTemplate(Command::StartMessage const &StartInfo,
                              std::filesystem::path const &filepath,
                              Metrics::IRegistrar *Registrar,
                              MetaData::TrackerPtr const &Tracker);

void addStreamSourceToWriterModule(
    std::vector<ModuleSettings> &StreamSettingsList, FileWriterTask &Task);

std::vector<ModuleHDFInfo>
initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString,
              std::filesystem::path const &template_path,
              bool const &is_legacy_writing);

/// \brief Extract information about the module (stream or link).
///
/// \param StreamInfo
/// \return The module information.
ModuleSettings
extractModuleInformationFromJsonForSource(ModuleHDFInfo const &ModuleInfo);

std::vector<ModuleSettings> extractModuleInformationFromJson(
    std::vector<ModuleHDFInfo> const &ModuleHDFInfoList);

std::unique_ptr<WriterModule::Base>
generateWriterInstance(ModuleSettings const &StreamInfo);

void setWriterHDFAttributes(hdf5::node::Group &RootNode,
                            ModuleSettings const &StreamInfo);

/// \brief Extract all mdat modules from the list of modules.
///
/// \param AllModules
/// \return The mdat modules.
std::vector<ModuleHDFInfo>
extractMdatModules(std::vector<ModuleHDFInfo> &Modules);

} // namespace FileWriter
