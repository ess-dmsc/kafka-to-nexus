// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file  CommandHandler.cpp

#include "CommandHandler.h"
#include "EventLogger.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "StreamMaster.h"
#include "Msg.h"
#include "json.h"
#include <algorithm>
#include <chrono>
#include <future>

using std::vector;

namespace FileWriter {

using nlohmann::json;

/// Helper to throw a common error message type.
static void throwMissingKey(std::string const &Key,
                            std::string const &Context) {
  throw std::runtime_error(fmt::format("Missing key {} from {}", Key, Context));
}

std::chrono::milliseconds findTime(json const &Document,
                                   std::string const &Key) {
  if (auto x = find<uint64_t>(Key, Document)) {
    std::chrono::milliseconds Time(x.inner());
    if (Time.count() != 0) {
      return Time;
    }
  }
  return std::chrono::milliseconds{-1};
}

std::vector<StreamHDFInfo>
CommandHandler::initializeHDF(FileWriterTask &Task,
                              std::string const &NexusStructureString,
                              bool UseSwmr) {
  json NexusStructure = json::parse(NexusStructureString);
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  json ConfigFile = json::parse("{}");
  Task.InitialiseHdf(NexusStructure.dump(), ConfigFile.dump(),
                     StreamHDFInfoList, UseSwmr);
  return StreamHDFInfoList;
}

/// \brief Extract information about the stream.
///
/// Extract the information about the stream from the json command and calls
/// the corresponding HDF writer modules to set up initial HDF structures
/// in the output file.
///
/// \param Task The task which will write the HDF file.
/// \param StreamHDFInfoList
/// \param Logger Pointer to spdlog instance to be used for logging.
///
/// \return The stream information.
static StreamSettings extractStreamInformationFromJsonForSource(
    std::unique_ptr<FileWriterTask> const &Task,
    StreamHDFInfo const &StreamInfo, SharedLogger Logger) {
  StreamSettings StreamSettings;
  StreamSettings.StreamHDFInfoObj = StreamInfo;

  json ConfigStream;
  ConfigStream = json::parse(StreamInfo.ConfigStream);

  json ConfigStreamInner;
  if (auto StreamMaybe = find<json>("stream", ConfigStream)) {
    ConfigStreamInner = StreamMaybe.inner();
  } else {
    throwMissingKey("stream", ConfigStream.dump());
  }

  StreamSettings.ConfigStreamJson = ConfigStreamInner.dump();
  Logger->info("Adding stream: {}", StreamSettings.ConfigStreamJson);

  if (auto TopicMaybe = find<json>("topic", ConfigStreamInner)) {
    StreamSettings.Topic = TopicMaybe.inner();
  } else {
    throwMissingKey("topic", ConfigStreamInner.dump());
  }

  if (auto SourceMaybe = find<std::string>("source", ConfigStreamInner)) {
    StreamSettings.Source = SourceMaybe.inner();
  } else {
    throwMissingKey("source", ConfigStreamInner.dump());
  }

  if (auto WriterModuleMaybe =
          find<std::string>("writer_module", ConfigStreamInner)) {
    StreamSettings.Module = WriterModuleMaybe.inner();
  } else {
    // Allow the old key name as well:
    if (auto ModuleMaybe = find<std::string>("module", ConfigStreamInner)) {
      StreamSettings.Module = ModuleMaybe.inner();
      Logger->debug("The key \"stream.module\" is deprecated, please use "
                    "\"stream.writer_module\" instead.");
    } else {
      throwMissingKey("writer_module", ConfigStreamInner.dump());
    }
  }

  if (auto RunParallelMaybe = find<bool>("run_parallel", ConfigStream)) {
    StreamSettings.RunParallel = RunParallelMaybe.inner();
  }
  if (StreamSettings.RunParallel) {
    Logger->info("Run parallel for source: {}", StreamSettings.Source);
  }

  HDFWriterModuleRegistry::ModuleFactory ModuleFactory;
  try {
    ModuleFactory = HDFWriterModuleRegistry::find(StreamSettings.Module);
  } catch (std::exception const &E) {
    throw std::runtime_error(
        fmt::format("Error while getting '{}',  source: {}  what: {}",
                    StreamSettings.Module, StreamSettings.Source, E.what()));
  }

  auto HDFWriterModule = ModuleFactory();
  if (!HDFWriterModule) {
    throw std::runtime_error(fmt::format(
        "Can not create a HDFWriterModule for '{}'", StreamSettings.Module));
  }

  auto RootGroup = Task->hdfGroup();
  try {
    HDFWriterModule->parse_config(ConfigStreamInner.dump(), "{}");
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Exception while HDFWriterModule::parse_config  module: {} "
                    " source: {}  what: {}",
                    StreamSettings.Module, StreamSettings.Source, E.what())));
  }
  auto Attributes = json::object();
  if (auto x = find<json>("attributes", ConfigStream)) {
    Attributes = x.inner();
  }
  auto StreamGroup = hdf5::node::get_group(RootGroup, StreamInfo.HDFParentName);
  HDFWriterModule->init_hdf({StreamGroup}, Attributes.dump());
  HDFWriterModule->close();
  HDFWriterModule.reset();
  return StreamSettings;
}

/// Helper to extract information about the provided streams.
/// \param Logger Pointer to spdlog instance to be used for logging.
static vector<StreamSettings>
extractStreamInformationFromJson(std::unique_ptr<FileWriterTask> const &Task,
                                 std::vector<StreamHDFInfo> &StreamHDFInfoList,
                                 SharedLogger Logger) {
  Logger->info("Command contains {} streams", StreamHDFInfoList.size());
  std::vector<StreamSettings> StreamSettingsList;
  for (auto &StreamHDFInfo : StreamHDFInfoList) {
    try {
      StreamSettingsList.push_back(extractStreamInformationFromJsonForSource(
          Task, StreamHDFInfo, Logger));
      StreamHDFInfo.InitialisedOk = true;
    } catch (json::parse_error const &E) {
      Logger->warn("Invalid json: {}", StreamHDFInfo.ConfigStream);
      continue;
    } catch (std::runtime_error const &E) {
      Logger->warn("Exception while initialising writer module  what: {}  "
                   "parent: {}  json: {}",
                   E.what(), StreamHDFInfo.HDFParentName,
                   StreamHDFInfo.ConfigStream);
      continue;
    } catch (...) {
      Logger->error("Unknown error caught while trying to initialise stream  "
                    "parent: {}  json: {}",
                    StreamHDFInfo.HDFParentName, StreamHDFInfo.ConfigStream);
    }
  }
  return StreamSettingsList;
}

void CommandHandler::handleNew(const json &JSONCommand,
                               std::chrono::milliseconds StartTime) {
  using nlohmann::detail::out_of_range;

  std::shared_ptr<KafkaW::ProducerTopic> StatusProducer;
  if (MasterPtr != nullptr) {
    StatusProducer = MasterPtr->getStatusProducer();
  }
  auto Task =
      std::make_unique<FileWriterTask>(Config.ServiceID, StatusProducer);
  if (auto x = find<std::string>("job_id", JSONCommand)) {
    std::string JobID = x.inner();
    if (JobID.empty()) {
      throwMissingKey("job_id", JSONCommand.dump());
    }

    if (MasterPtr != nullptr) { // workaround to prevent seg fault in tests
      if (MasterPtr->getStreamMasterForJobID(JobID) != nullptr) {
        Logger->error("Command ignored as job id {} is already in progress",
                      JobID);
        return;
      }
    }

    Task->setJobId(JobID);
  } else {
    throwMissingKey("job_id", JSONCommand.dump());
  }

  if (MasterPtr != nullptr) {
    logEvent(MasterPtr->getStatusProducer(), StatusCode::Start,
             Config.ServiceID, Task->jobID(), "Start job");
  }

  uri::URI Broker("localhost:9092");
  if (auto BrokerStringMaybe = find<std::string>("broker", JSONCommand)) {
    auto BrokerString = BrokerStringMaybe.inner();
    try {
      Broker.parse(BrokerString);
    } catch (std::runtime_error &e) {
      Logger->warn("Unable to parse broker {} in command message, using "
                   "default broker (localhost:9092)",
                   BrokerString);
    }
    Logger->trace("Use main broker: {}", Broker.HostPort);
  }

  if (auto FileAttributesMaybe = find<json>("file_attributes", JSONCommand)) {
    if (auto FileNameMaybe =
            find<std::string>("file_name", FileAttributesMaybe.inner())) {
      Task->setFilename(Config.HDFOutputPrefix, FileNameMaybe.inner());
    } else {
      throwMissingKey("file_attributes.file_name", JSONCommand.dump());
    }
  } else {
    throwMissingKey("file_attributes", JSONCommand.dump());
  }

  bool UseSwmr = true;
  if (auto UseHDFSWMRMaybe = find<bool>("use_hdf_swmr", JSONCommand)) {
    UseSwmr = UseHDFSWMRMaybe.inner();
  }

  // When FileWriterTask::InitialiseHdf() returns, `stream_hdf_info` will
  // contain the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  if (auto NexusStructureMaybe = find<json>("nexus_structure", JSONCommand)) {
    try {
      StreamHDFInfoList =
          initializeHDF(*Task, NexusStructureMaybe.inner().dump(), UseSwmr);
    } catch (std::runtime_error const &E) {
      std::throw_with_nested(std::runtime_error(
          fmt::format("Failed to initializeHDF: {}", E.what())));
    }
  } else {
    throwMissingKey("nexus_structure", JSONCommand.dump());
  }

  std::vector<StreamSettings> StreamSettingsList =
      extractStreamInformationFromJson(Task, StreamHDFInfoList, Logger);

  if (auto ThrowOnUninitialisedStreamMaybe =
          find<bool>("abort_on_uninitialised_stream", JSONCommand)) {
    if (ThrowOnUninitialisedStreamMaybe.inner()) {
      for (auto const &Item : StreamHDFInfoList) {
        // cppcheck-suppress useStlAlgorithm
        if (!Item.InitialisedOk) {
          throw std::runtime_error(fmt::format("Could not initialise {}  {}",
                                               Item.HDFParentName,
                                               Item.ConfigStream));
        }
      }
    }
  }

  addStreamSourceToWriterModule(StreamSettingsList, Task);
  Config.StreamerConfiguration.StartTimestamp =
      std::chrono::milliseconds::zero();
  Config.StreamerConfiguration.StopTimestamp =
      std::chrono::milliseconds::zero();

  // If start time not specified use command message timestamp
  std::chrono::milliseconds Time = findTime(JSONCommand, "start_time");
  if (Time.count() > 0) {
    Config.StreamerConfiguration.StartTimestamp = Time;
  } else {
    Config.StreamerConfiguration.StartTimestamp = StartTime;
  }
  Logger->info("Start time: {}ms",
               Config.StreamerConfiguration.StartTimestamp.count());
  Time = findTime(JSONCommand, "stop_time");
  if (Time.count() > 0) {
    Config.StreamerConfiguration.StopTimestamp = Time;
    Logger->info("Stop time: {}ms",
                 Config.StreamerConfiguration.StopTimestamp.count());
  }

  if (MasterPtr != nullptr) {
    // Register the task with master.
    Logger->info("Write file with job_id: {}", Task->jobID());
    auto s = StreamMaster::createStreamMaster(Broker.HostPort, std::move(Task),
                                              Config,
                                              MasterPtr->getStatusProducer());
    if (auto status_producer = MasterPtr->getStatusProducer()) {
      s->report(std::chrono::milliseconds{Config.StatusMasterIntervalMS});
    }
    if (Config.topic_write_duration.count() != 0) {
      s->setTopicWriteDuration(Config.topic_write_duration);
    }
    s->start();

    MasterPtr->addStreamMaster(std::move(s));
  } else {
    FileWriterTasks.emplace_back(std::move(Task));
  }
}

void CommandHandler::addStreamSourceToWriterModule(
    std::vector<StreamSettings> &StreamSettingsList,
    std::unique_ptr<FileWriterTask> &Task) {
  auto Logger = getLogger();
  bool UseParallelWriter = false;

  for (auto const &StreamSettings : StreamSettingsList) {
    if (!UseParallelWriter || !StreamSettings.RunParallel) {
      Logger->trace("add Source as non-parallel: {}", StreamSettings.Topic);
      HDFWriterModuleRegistry::ModuleFactory ModuleFactory;

      try {
        ModuleFactory = HDFWriterModuleRegistry::find(StreamSettings.Module);
      } catch (std::exception const &E) {
        Logger->info("Module '{}' is not available, error {}",
                     StreamSettings.Module, E.what());
        continue;
      }

      auto HDFWriterModule = ModuleFactory();
      if (!HDFWriterModule) {
        Logger->info("Can not create a HDFWriterModule for '{}'",
                     StreamSettings.Module);
        continue;
      }

      try {
        // Reopen the previously created HDF dataset.
        HDFWriterModule->parse_config(StreamSettings.ConfigStreamJson, "{}");
        try {
          auto RootGroup = Task->hdfGroup();
          auto StreamGroup = hdf5::node::get_group(
              RootGroup, StreamSettings.StreamHDFInfoObj.HDFParentName);
          auto Err = HDFWriterModule->reopen({StreamGroup});
          if (Err != HDFWriterModule_detail::InitResult::OK) {
            Logger->error("can not reopen HDF file for stream {}",
                          StreamSettings.StreamHDFInfoObj.HDFParentName);
            continue;
          }
        } catch (std::runtime_error const &e) {
          Logger->error("Exception on HDFWriterModule->reopen(): {}", e.what());
          continue;
        }

        // Create a Source instance for the stream and add to the task.
        Source ThisSource(StreamSettings.Source, StreamSettings.Module,
                          move(HDFWriterModule));
        ThisSource.setTopic(StreamSettings.Topic);
        Task->addSource(std::move(ThisSource));
      } catch (std::runtime_error const &E) {
        Logger->warn(
            "Exception while initializing writer module {} for source {}: {}",
            StreamSettings.Module, StreamSettings.Source, E.what());
        continue;
      }
    }
  }
}

void CommandHandler::handleFileWriterTaskClearAll() {
  if (MasterPtr != nullptr) {
    MasterPtr->stopStreamMasters();
  }
  FileWriterTasks.clear();
}

void CommandHandler::handleExit() {
  if (MasterPtr != nullptr) {
    MasterPtr->stop();
  }
}

void CommandHandler::handleStreamMasterStop(const json &Command) {
  Logger->trace("{}", Command.dump());
  std::string JobID;
  if (auto x = find<std::string>("job_id", Command)) {
    JobID = x.inner();
  } else {
    throwMissingKey("job_id", Command.dump());
  }

  std::chrono::milliseconds StopTime = findTime(Command, "stop_time");
  if (MasterPtr != nullptr) {
    auto &StreamMaster = MasterPtr->getStreamMasterForJobID(JobID);
    if (StreamMaster != nullptr) {
      if (StopTime.count() > 0) {
        Logger->info(
            "Received request to gracefully stop file with id : {} at {} ms",
            JobID, StopTime.count());
        StreamMaster->setStopTime(StopTime);
      } else {
        Logger->info("Received request to gracefully stop file with id : {}",
                     JobID);
        StreamMaster->requestStop();
      }
    } else {
      Logger->warn("Can not find StreamMaster for JobID: {}", JobID);
    }
  }
}

/// \brief Parse the given command and pass it on to a more specific
/// handler.
///
/// \param Command The command to parse.
/// \param MsgTimestamp The message timestamp.
void CommandHandler::handle(std::string const &Command,
                            const std::chrono::milliseconds StartTime) {
  json JSONCommand;
  try {
    JSONCommand = json::parse(Command);
  } catch (...) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Can not parse command: {}", TruncateCommand(Command))));
  }

  if (auto ServiceIDMaybe = find<std::string>("service_id", JSONCommand)) {
    if (ServiceIDMaybe.inner() != Config.ServiceID) {
      Logger->trace("Ignoring command addressed to service_id: {}",
                    ServiceIDMaybe.inner());
      return;
    }
  }

  uint64_t TeamId = 0;
  uint64_t CommandTeamId = 0;
  if (auto x = find<uint64_t>("teamid", JSONCommand)) {
    CommandTeamId = x.inner();
  }
  if (CommandTeamId != TeamId) {
    Logger->info("INFO command is for teamid {:016x}, we are {:016x}",
                 CommandTeamId, TeamId);
    return;
  }

  if (auto CmdMaybe = find<std::string>("cmd", JSONCommand)) {
    std::string CommandMain = CmdMaybe.inner();
    std::transform(CommandMain.begin(), CommandMain.end(), CommandMain.begin(),
                   ::tolower);
    if (CommandMain == "filewriter_new") {
      handleNew(JSONCommand, StartTime);
      return;
    } else if (CommandMain == "filewriter_exit") {
      handleExit();
      return;
    } else if (CommandMain == "filewriter_stop") {
      handleStreamMasterStop(JSONCommand);
      return;
    } else if (CommandMain == "file_writer_tasks_clear_all") {
      if (auto y = find<std::string>("recv_type", JSONCommand)) {
        std::string ReceiverType = y.inner();
        if (ReceiverType == "FileWriter") {
          handleFileWriterTaskClearAll();
          return;
        }
      } else {
        throwMissingKey("recv_type", JSONCommand.dump());
      }

    } else {
      throw std::runtime_error(
          fmt::format("Could not understand 'cmd' field of this command."));
    }
  } else {
    throw std::runtime_error(
        fmt::format("Can not extract 'cmd' from command."));
  }
}

std::string format_nested_exception(std::exception const &E,
                                    std::stringstream &StrS, int Level) {
  if (Level > 0) {
    StrS << '\n';
  }
  StrS << fmt::format("{:{}}{}", "", 2 * Level, E.what());
  try {
    std::rethrow_if_nested(E);
  } catch (std::exception const &E) {
    format_nested_exception(E, StrS, Level + 1);
  } catch (...) {
  }
  return StrS.str();
}

std::string format_nested_exception(std::exception const &E) {
  std::stringstream StrS;
  return format_nested_exception(E, StrS, 0);
}

/// Truncate logged command so that it doesn't saturate logs.
///
/// \param Command Original command that threw an error
/// \return shorter version to be written in logs.
std::string TruncateCommand(std::string const &Command) {

  unsigned int MaxCmdSize = 1500;
  if (Command.size() > MaxCmdSize) {
    auto TruncatedCommand = Command.substr(0, MaxCmdSize);
    TruncatedCommand.append("\n  [...]\n Command was truncated, displayed "
                            "first 1500 characters.\n");
    return TruncatedCommand;
  }
  return Command;
}

void CommandHandler::tryToHandle(std::string const &Command,
                                 std::chrono::milliseconds MsgTimestamp) {
  try {
    handle(Command, MsgTimestamp);
  } catch (...) {
    std::string JobID = "unknown";
    try {
      JobID = json::parse(Command)["job_id"];
    } catch (...) {
      // Okay to ignore as original exception will give the reason.
    }

    try {
      std::throw_with_nested(
          std::runtime_error("Error in CommandHandler::tryToHandle"));
    } catch (std::runtime_error const &E) {
      auto TruncatedCommand = TruncateCommand(Command);
      auto Message = fmt::format(
          "Unexpected std::exception while handling command:\n{}\n{}",
          format_nested_exception(E), TruncatedCommand);
      Logger->error("JobID: {}  StatusCode: {}  Message: {}", JobID,
                    convertStatusCodeToString(StatusCode::Fail), Message);
      if (MasterPtr != nullptr) {
        logEvent(MasterPtr->getStatusProducer(), StatusCode::Fail,
                 Config.ServiceID, JobID, Message);
      }
    }
  }
}

size_t CommandHandler::getNumberOfFileWriterTasks() const {
  return FileWriterTasks.size();
}

std::unique_ptr<FileWriterTask> &
CommandHandler::getFileWriterTaskByJobID(std::string const &JobID) {
  auto Task = std::find_if(
      FileWriterTasks.begin(), FileWriterTasks.end(),
      [&JobID](auto const &FwTask) { return FwTask->jobID() == JobID; });

  if (Task != FileWriterTasks.end()) {
    return *Task;
  }
  throw std::out_of_range("Unable to find task by Job ID");
}

} // namespace FileWriter
