#pragma once

#include "FileWriterTask.h"
#include "MainOpt.h"
#include "MasterInterface.h"
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

/// \brief If fails to parse the `Command`, adds error info and throws
/// exception.
///
/// \param  Command Command passed to the program.
/// \param Logger Pointer to spdlog instance to be used for logging.
///
/// \return If parsing successful returns `nlohmann::json`, otherwise throws an
/// exception.
nlohmann::json parseOrThrow(std::string const &Command,
                            SharedLogger Logger);

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
  CommandHandler(MainOpt &Settings, MasterInterface *Master);

  /// \brief Given a JSON string, create a new file writer task.
  ///
  /// \param Command Command for configuring the new task.
  void handleNew(std::string const &Command,
                 std::chrono::milliseconds MsgTimestamp);

  /// Stop the whole file writer application.
  void handleExit();

  /// Stop and remove all ongoing file writer jobs.
  void handleFileWriterTaskClearAll();

  /// \brief Stop a given job.
  ///
  /// \param Command The command defining which job to stop.
  void handleStreamMasterStop(std::string const &Command);

  /// \brief Try to handle the message.
  ///
  /// \param Msg The message.
  /// \param MsgTimestamp The rd_kafka_message_timestamp.
  void tryToHandle(std::unique_ptr<Msg> Message,
                   std::chrono::milliseconds MsgTimestampMilliseconds =
                       std::chrono::milliseconds{-1});

  /// \brief Try to handle the command.
  ///
  /// \param Command The command to parse.
  /// \param MsgTimestamp The rd_kafka_message_timestamp.
  void tryToHandle(
      std::string const &Command,
      std::chrono::milliseconds MsgTimestamp = std::chrono::milliseconds{-1});

  /// \brief Get number of active writer tasks.
  ///
  /// \return  Number of active writer tasks.
  size_t getNumberOfFileWriterTasks() const;

  /// \brief Find a writer task given its `JobID`.
  ///
  /// \param JobID The job id to find.
  /// \return The writer task.
  std::unique_ptr<FileWriterTask> &
  getFileWriterTaskByJobID(std::string const &JobID);

private:
  /// \brief Parse the given command and pass it on to a more specific
  /// handler.
  ///
  /// \param Command The command to parse.
  /// \param MsgTimestamp The message timestamp.
  void handle(std::string const &command,
              std::chrono::milliseconds MsgTimestamp);

  static void
  addStreamSourceToWriterModule(std::vector<StreamSettings> &StreamSettingsList,
                                std::unique_ptr<FileWriterTask> &Task);

  static std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task, std::string const &NexusStructureString,
                bool UseSwmr);
  MainOpt &Config;
  MasterInterface *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
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
