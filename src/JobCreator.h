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

std::unique_ptr<IStreamController>
createFileWritingJob(Command::StartInfo const &StartInfo, MainOpt &Settings,
                     Metrics::IRegistrar *Registrar,
                     MetaData::TrackerPtr const &Tracker);

// Note: The functions below are "private" helper functions.

void addStreamSourceToWriterModule(
    std::vector<ModuleSettings> &StreamSettingsList, FileWriterTask &Task);

std::vector<ModuleHDFInfo>
initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString);

/// \brief Extract information about the module (stream or link).
///
/// \param StreamInfo
/// \return The module information.
ModuleSettings
extractModuleInformationFromJsonForSource(ModuleHDFInfo const &ModuleInfo);

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
