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
};

class IJobCreator {
public:
  virtual std::unique_ptr<IStreamController>
  createFileWritingJob(Command::StartInfo const &StartInfo, MainOpt &Settings,
                       SharedLogger const &Logger,
                       Metrics::Registrar Registrar, MetaData::TrackerPtr const &Tracker) = 0;
  virtual ~IJobCreator() = default;
};

class JobCreator : public IJobCreator {
public:
  /// \brief Create a new file-writing job.
  ///
  /// \param StartInfo The details for starting the job.
  /// \param StatusProducer The producer for the job to report its status on.
  /// \param Settings General settings for the file writer.
  /// \param Logger The logger.
  /// \return The new file-writing job.
  std::unique_ptr<IStreamController>
  createFileWritingJob(Command::StartInfo const &StartInfo, MainOpt &Settings,
                       SharedLogger const &Logger,
                       Metrics::Registrar Registrar, MetaData::TrackerPtr const &Tracker) override;

private:
  static void addStreamSourceToWriterModule(
      std::vector<StreamSettings> const &StreamSettingsList,
      std::unique_ptr<FileWriterTask> &Task);

  static std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString);
};

/// \brief Extract information about the stream.
///
/// \param StreamInfo
/// \return The stream information.
StreamSettings
extractStreamInformationFromJsonForSource(StreamHDFInfo const &StreamInfo);

} // namespace FileWriter
