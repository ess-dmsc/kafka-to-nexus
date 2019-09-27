// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FileWriterTask.h"
#include "CommandParser.h"
#include "MainOpt.h"
#include "MasterInterface.h"
#include "StreamsController.h"
#include "json.h"
#include <memory>

namespace FileWriter {

/// \brief Holder for the stream settings.
struct StreamSettings {
  StreamHDFInfo StreamHDFInfoObj;
  std::string Topic;
  std::string Module;
  std::string Source;
  bool RunParallel = false;
  std::string ConfigStreamJson;
  std::string Attributes;
};


class CommandHandler {
public:
  /// \brief Create a new file-writing job.
  ///
  /// \param StartInfo The details for starting the job.
  /// \param StatusProducer The producer for the job to report its status on.
  /// \param Settings General settings for the file writer.
  /// \return The new file-writing job.
  std::unique_ptr<IStreamMaster> createFileWritingJob(StartCommandInfo const &StartInfo,
      std::shared_ptr<KafkaW::ProducerTopic> const &StatusProducer, MainOpt &Settings);

private:

  static void
  addStreamSourceToWriterModule(std::vector<StreamSettings> &StreamSettingsList,
                                std::unique_ptr<FileWriterTask> &Task);

  static std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString,
                bool UseSwmr);
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
