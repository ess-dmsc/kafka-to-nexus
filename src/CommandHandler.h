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
};

std::string format_nested_exception(std::exception const &E);

std::string format_nested_exception(std::exception const &E,
                                    std::stringstream &StrS, int Level);

/// Interpret and execute received commands.
class CommandHandler {
public:
  /// \brief Initialize a new `CommandHandler`.
  ///
  /// \param Config Configuration of the file writer.
  /// \param MasterPtr Optional `Master` which can continue to watch over newly
  /// created jobs. Not used for example in some tests.
  CommandHandler(MainOpt &Settings, std::shared_ptr<StreamsController> Master, std::shared_ptr<KafkaW::ProducerTopic> Producer)
      : Config(Settings), StreamControl(std::move(Master)), StatusProducer(std::move(Producer)){};

  /// \brief Create a new file writer task.
  ///
  /// \param JSONCommand Command for configuring the new task.
  void handleNew(const nlohmann::json &JSONCommand,
                 std::chrono::milliseconds StartTime);

  /// Stop the whole file writer application.
  void handleExit();

  /// Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// \brief Stop a given job.
  ///
  /// \param Command The command defining which job to stop.
  void handleStreamMasterStop(const nlohmann::json &Command);

  /// \brief Try to handle the command.
  ///
  /// \param Command The command to parse.
  /// \param MsgTimestamp The rd_kafka_message_timestamp.
  void tryToHandle(
      std::string const &Command,
      std::chrono::milliseconds MsgTimestamp = std::chrono::milliseconds{0});

private:
  void handle(std::string const &command,
              std::chrono::milliseconds MsgTimestamp);

  static void
  addStreamSourceToWriterModule(std::vector<StreamSettings> &StreamSettingsList,
                                std::unique_ptr<FileWriterTask> &Task);

  static std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString,
                bool UseSwmr);
  MainOpt &Config;
  std::shared_ptr<StreamsController> StreamControl;
  std::shared_ptr<KafkaW::ProducerTopic> StatusProducer;
  SharedLogger Logger = getLogger();
};

/// \brief Extract the time in milliseconds from the JSON.
///
/// \param Document The JSON document.
/// \param Key The time identifier keyword.
/// \return The value in milliseconds.
std::chrono::milliseconds findTime(nlohmann::json const &Document,
                                   std::string const &Key);

std::string TruncateCommand(std::string const &Command);
} // namespace FileWriter
