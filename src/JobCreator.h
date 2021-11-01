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
#include "ModuleSettings.h"
#include "MainOpt.h"
#include "Metrics/Registrar.h"
#include "StreamController.h"
#include "json.h"
#include <memory>

namespace FileWriter {

std::unique_ptr<IStreamController>
createFileWritingJob(Command::StartInfo const &StartInfo, MainOpt &Settings,
                     Metrics::Registrar Registrar,
                     MetaData::TrackerPtr const &Tracker);

// Note: The functions below are "private" helper functions.

void addStreamSourceToWriterModule(
    std::vector<ModuleSettings> &StreamSettingsList,
    std::unique_ptr<FileWriterTask> &Task);

std::vector<StreamHDFInfo>
initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString);

/// \brief Extract information about the stream or link.
///
/// \param StreamInfo
/// \return The stream or link information.
ModuleSettings
extractLinkAndStreamInformationFromJsonForSource(StreamHDFInfo const &StreamInfo);

std::unique_ptr<WriterModule::Base>
generateWriterInstance(ModuleSettings const &StreamInfo);

void setWriterHDFAttributes(hdf5::node::Group &RootNode,
                            ModuleSettings const &StreamInfo);

} // namespace FileWriter
