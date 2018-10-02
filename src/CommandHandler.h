#pragma once

#include "FileWriterTask.h"
#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include "MasterI.h"
#include "Msg.h"
#include "json.h"
#include <memory>

namespace FileWriter {

struct StreamSettings;

nlohmann::json parseOrThrow(std::string const &Command);

/// Interprets and execute commands received.
class CommandHandler {
public:
  CommandHandler(MainOpt &config, MasterI *master);

  /// \brief  Given a JSON string, create a new file writer task.
  ///
  /// \param Command The command for configuring the new task.
  void handleNew(std::string const &Command);

  /// \brief  Stop the whole file writer application.
  void handleExit();

  /// \brief  Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// \brief  Stops a given job.
  ///
  /// \param Command The command for defining which job to stop.
  void handleStreamMasterStop(std::string const &Command);

  /// \brief  Passes content of the message to the command handler.
  ///
  /// \param Msg The message.
  void tryToHandle(Msg const &msg);

  /// \brief  Parses the given command and passes it on to a more specific
  /// handler.
  ///
  /// \param Command The command to parse.
  void handle(std::string const &command);
  void tryToHandle(std::string const &Command);

  size_t getNumberOfFileWriterTasks() const;
  std::unique_ptr<FileWriterTask> &getFileWriterTaskByJobID(std::string JobID);

private:
  /// \brief  Configure the HDF writer modules for writing.
  ///
  /// \param StreamSettingsList The settings for the stream.
  /// \param Task The task to configure.
  void addStreamSourceToWriterModule(
      const std::vector<StreamSettings> &stream_settings_list,
      std::unique_ptr<FileWriterTask> &fwt);

  /// \brief  Set up the basic HDF file structure
  ///
  /// Given a task and the `nexus_structure` as json string, set up the
  /// basic HDF file structure.
  ///
  /// \param Task The task which will write the HDF file.
  /// \param NexusStructureString The structure of the NeXus file.
  /// \return The related stream settings.
  std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task,
                std::string const &NexusStructureString) const;
  MainOpt &Config;
  MasterI *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
};

std::string findBroker(std::string const &);

std::string format_nested_exception(std::exception const &E);

} // namespace FileWriter
