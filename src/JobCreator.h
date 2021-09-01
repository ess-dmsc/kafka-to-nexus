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
#include "StreamController.h"
#include "json.h"
#include <memory>

namespace FileWriter {

/// \brief Holder for the stream settings.
struct StreamSettings {
  StreamHDFInfo StreamHDFInfoObj;
  std::string Topic;
  std::string Module;
  std::string Source;
  std::string ConfigStreamJson;
  std::string Attributes;
  std::unique_ptr<WriterModule::Base> WriterModule;
};

std::unique_ptr<IStreamController>
createFileWritingJob(Command::StartInfo const &StartInfo, MainOpt &Settings,
                     Metrics::Registrar Registrar,
                     MetaData::TrackerPtr const &Tracker);

// Note: The functions below are "private" helper functions.

void addStreamSourceToWriterModule(
    std::vector<StreamSettings> &StreamSettingsList,
    std::unique_ptr<FileWriterTask> &Task);

std::vector<StreamHDFInfo>
initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString);

/// \brief Extract information about the stream.
///
/// \param StreamInfo
/// \return The stream information.
StreamSettings
extractStreamInformationFromJsonForSource(StreamHDFInfo const &StreamInfo);

std::unique_ptr<WriterModule::Base>
generateWriterInstance(StreamSettings const &StreamInfo);

void setWriterHDFAttributes(hdf5::node::Group &RootNode,
                            StreamSettings const &StreamInfo);

} // namespace FileWriter
