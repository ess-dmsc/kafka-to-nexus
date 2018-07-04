#pragma once

#include "MainOpt.h"
#include "Master.h"
#include <memory>

class T_CommandHandler;
class CommandHandler_Testing;

namespace FileWriter {

struct StreamSettings;

/// Interprets and execute commands received.
class CommandHandler {
public:
  CommandHandler(MainOpt &config, Master *master);

  /// Given a JSON string, create a new file writer task.
  ///
  /// \param Command The command for configuring the new task.
  void handleNew(std::string const &Command);

  /// Stop the whole file writer application.
  void handleExit();

  /// Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// Stops a given job.
  ///
  /// \param Command The command for defining which job to stop.
  void handleStreamMasterStop(std::string const &Command);

  /// Passes content of the message to the command handler.
  ///
  /// \param Msg The message.
  void handle(Msg const &msg);

  /// Parses the given command and passes it on to a more specific handler.
  ///
  /// \param Command The command to parse.
  void handle(std::string const &command);
  void tryToHandle(std::string const &Command);

private:
  /// Configure the HDF writer modules for writing.
  ///
  /// \param StreamSettingsList The settings for the stream.
  /// \param Task The task to configure.
  void addStreamSourceToWriterModule(
      const std::vector<StreamSettings> &stream_settings_list,
      std::unique_ptr<FileWriterTask> &fwt);

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
  Master *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
  friend class ::T_CommandHandler;
  friend class ::CommandHandler_Testing;
};

std::string findBroker(std::string const &);

} // namespace FileWriter
