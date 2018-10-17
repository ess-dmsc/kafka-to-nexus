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

/// \brief If fails to parse the `Command`, adds error info and throws
/// exception.
///
/// \param Command Command passed to the program.
/// \return If parsing successful returns `nlohmann::json`, otherwise throws an
/// exception.
nlohmann::json parseOrThrow(std::string const &Command);

std::string findBroker(std::string const &);

/// Formats exceptions into readable form.
std::string format_nested_exception(std::exception const &E);

/// Formats exceptions into readable form.
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
  CommandHandler(MainOpt &config, MasterI *master);

  /// \brief Given a JSON string, create a new file writer task.
  ///
  /// \param Command Command for configuring the new task.
  void handleNew(std::string const &Command);

  /// Stop the whole file writer application.
  void handleExit();

  /// Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// \brief Stop a given job.
  ///
  /// \param Command The command defining which job to stop.
  void handleStreamMasterStop(std::string const &Command);

  /// \brief Pass content of the message to the command handler.
  ///
  /// \param Msg The message.
  void tryToHandle(Msg const &msg);

  /// \brief Parse the given command and pass it on to a more specific
  /// handler.
  ///
  /// \param Command The command to parse.
  void handle(std::string const &command);

  /// Try to handle command and catch exceptions
  void tryToHandle(std::string const &Command);

  /// \brief Get number of active writer tasks.
  ///
  /// \return  Number of active writer tasks.
  size_t getNumberOfFileWriterTasks() const;

  /// \brief Find a writer task given its `JobID`.
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
} // namespace FileWriter
