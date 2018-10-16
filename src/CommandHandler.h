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

/// Helper for adding more error information on parse error.
nlohmann::json parseOrThrow(std::string const &Command);

/// Interpret and execute commands received.
class CommandHandler {
public:
  /// Initialize a new `CommandHandler`.
  ///
  /// \param Config Configuration of the file writer.
  /// \param MasterPtr Optional `Master` which can continue to watch over newly
  /// created jobs. Not used for example in some tests.
  CommandHandler(MainOpt &config, MasterI *master);

  /// Given a JSON string, create a new file writer task.
  ///
  /// \param Command The command for configuring the new task.
  void handleNew(std::string const &Command);

  /// Stop the whole file writer application.
  void handleExit();

  /// Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// Stop a given job.
  ///
  /// \param Command The command defining which job to stop.
  void handleStreamMasterStop(std::string const &Command);

  /// Pass content of the message to the command handler.
  ///
  /// \param Msg The message.
  void tryToHandle(Msg const &msg);

  /// Parse the given command and pass it on to a more specific
  /// handler.
  ///
  /// \param Command The command to parse.
  void handle(std::string const &command);

  /// Try to handle command and catch exceptions
  void tryToHandle(std::string const &Command);

  /// Get number of active writer tasks.
  ///
  /// \return  Number of active writer tasks.
  size_t getNumberOfFileWriterTasks() const;

  /// Find a writer task given its `JobID`.
  ///
  /// \return  The writer task.
  std::unique_ptr<FileWriterTask> &getFileWriterTaskByJobID(std::string JobID);

private:
  void addStreamSourceToWriterModule(
      const std::vector<StreamSettings> &stream_settings_list,
      std::unique_ptr<FileWriterTask> &fwt);

  std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task,
                std::string const &NexusStructureString) const;
  MainOpt &Config;
  MasterI *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
};

std::string findBroker(std::string const &);

std::string format_nested_exception(std::exception const &E);
std::string format_nested_exception(std::exception const &E,
                                    std::stringstream &StrS, int Level);

} // namespace FileWriter
