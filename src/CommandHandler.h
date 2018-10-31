#pragma once

#include "FileWriterTask.h"
#include "KafkaW/KafkaW.h"
#include "MainOpt.h"
#include "MasterI.h"
#include "Msg.h"
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

/// \brief Helper for adding more error information on parse error.
nlohmann::json parseOrThrow(std::string const &Command);

/// \brief Interprets and execute commands received.
class CommandHandler {
public:
  /// \brief Constructor.
  ///
  /// \param Config Configuration of the file writer.
  /// \param MasterPtr Optional `Master` which can continue to watch over newly
  /// created jobs. Not used for example in some tests.
  CommandHandler(MainOpt &config, MasterI *master);

  /// \brief Handle the command which starts writing of a file.
  ///
  /// \param Command The command for configuring the new task.
  void handleNew(std::string const &Command);

  /// \brief Handle command to terminate the program.
  void handleExit();

  /// \brief Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// \brief Handle command to stop a specific job.
  ///
  /// \param Command The command for defining which job to stop.
  void handleStreamMasterStop(std::string const &Command);

  /// \brief Try to handle command and catch exceptions.
  ///
  /// \param Msg The message.
  void tryToHandle(Msg const &msg);

  /// \brief Parses the given command and passes it on to a more specific
  /// handler.
  ///
  /// \param Command The command to parse.
  void handle(std::string const &command);

  void tryToHandle(std::string const &Command);

  /// \brief Get number of active writer tasks.
  ///
  /// \return Number of active writer tasks.
  size_t getNumberOfFileWriterTasks() const;

  /// \brief Find a writer task given its `JobID`.
  ///
  /// \param JobID The job id to find.
  ///
  /// \return The writer task.
  std::unique_ptr<FileWriterTask> &getFileWriterTaskByJobID(std::string JobID);

private:
  /// \brief Add writer modules for the streams defined in nexus structure.
  ///
  /// \param StreamSettingsList The settings for the stream.
  /// \param Task The task to configure.
  void addStreamSourceToWriterModule(
      const std::vector<StreamSettings> &stream_settings_list,
      std::unique_ptr<FileWriterTask> &fwt);

  /// \brief Set up the basic HDF file structure
  ///
  /// Given a task and the `nexus_structure` as json string, set up the
  /// basic HDF file structure.
  ///
  /// \param Task The task which will write the HDF file.
  /// \param NexusStructureString The structure of the NeXus file.
  ///
  /// \return The related stream settings.
  std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString,
                bool UseSwmr) const;
  MainOpt &Config;
  MasterI *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
};

/// \brief Helper function to get nicer error messages.
/// \param E The exception to format.
///
/// \return Formatted exception message(s).
std::string format_nested_exception(std::exception const &E);

} // namespace FileWriter
