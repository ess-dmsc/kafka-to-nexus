/// \file  CommandHandler.cpp

#include "CommandHandler.h"
#include "EventLogger.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "StreamMaster.h"
#include "Streamer.h"
#include "helper.h"
#include "json.h"
#include <algorithm>
#include <chrono>
#include <future>
#include <sstream>

using std::array;
using std::vector;

namespace FileWriter {

using nlohmann::json;

json parseOrThrow(std::string const &Command) {
  try {
    return json::parse(Command);
  } catch (json::parse_error const &E) {
    LOG(Sev::Warning, "Can not parse command  what: {}  Command: {}", E.what(),
        Command);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Can not parse command  what: {}  Command: {}", E.what(), Command)));
  }
}

/// Helper to throw a common error message type.
static void throwMissingKey(std::string const &Key,
                            std::string const &Context) {
  throw std::runtime_error(fmt::format("Missing key {} from {}", Key, Context));
}

CommandHandler::CommandHandler(MainOpt &Config_, MasterI *MasterPtr_)
    : Config(Config_), MasterPtr(MasterPtr_) {}

/// \brief Parse the given `NexusStructureString`
///
/// Parse the given `NexusStructureString` and call the initialization of the
/// HDF structures.
std::vector<StreamHDFInfo>
CommandHandler::initializeHDF(FileWriterTask &Task,
                              std::string const &NexusStructureString,
                              bool UseSwmr) const {
  using nlohmann::json;
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
///
/// \return The stream information.
static StreamSettings extractStreamInformationFromJsonForSource(
    std::unique_ptr<FileWriterTask> const &Task,
    StreamHDFInfo const &StreamHDFInfo) {
  using nlohmann::json;
  StreamSettings StreamSettings;
  StreamSettings.StreamHDFInfoObj = StreamHDFInfo;

  json ConfigStream;
  ConfigStream = json::parse(StreamHDFInfo.ConfigStream);

  json ConfigStreamInner;
  if (auto StreamMaybe = find<json>("stream", ConfigStream)) {
    ConfigStreamInner = StreamMaybe.inner();
  } else {
    throwMissingKey("stream", ConfigStream.dump());
  }

  StreamSettings.ConfigStreamJson = ConfigStreamInner.dump();
  LOG(Sev::Info, "Adding stream: {}", StreamSettings.ConfigStreamJson);

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
      LOG(Sev::Notice, "The key \"stream.module\" is deprecated, please use "
                       "\"stream.writer_module\" instead.");
    } else {
      throwMissingKey("writer_module", ConfigStreamInner.dump());
    }
  }

  if (auto RunParallelMaybe = find<bool>("run_parallel", ConfigStream)) {
    StreamSettings.RunParallel = RunParallelMaybe.inner();
  }
  if (StreamSettings.RunParallel) {
    LOG(Sev::Info, "Run parallel for source: {}", StreamSettings.Source);
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
  auto StreamGroup =
      hdf5::node::get_group(RootGroup, StreamHDFInfo.HDFParentName);
  HDFWriterModule->init_hdf({StreamGroup}, Attributes.dump());
  HDFWriterModule->close();
  HDFWriterModule.reset();
  return StreamSettings;
}

/// Helper to extract information about the provided streams.
static std::vector<StreamSettings> extractStreamInformationFromJson(
    std::unique_ptr<FileWriterTask> const &Task,
    std::vector<StreamHDFInfo> &StreamHDFInfoList) {
  LOG(Sev::Info, "Command contains {} streams", StreamHDFInfoList.size());
  std::vector<StreamSettings> StreamSettingsList;
  for (auto &StreamHDFInfo : StreamHDFInfoList) {
    try {
      StreamSettingsList.push_back(
          extractStreamInformationFromJsonForSource(Task, StreamHDFInfo));
      StreamHDFInfo.InitialisedOk = true;
    } catch (json::parse_error const &E) {
      LOG(Sev::Warning, "Invalid json: {}", StreamHDFInfo.ConfigStream);
      continue;
    } catch (std::runtime_error const &E) {
      LOG(Sev::Warning, "Exception while initialising writer module  what: {}  "
                        "parent: {}  json: {}",
          E.what(), StreamHDFInfo.HDFParentName, StreamHDFInfo.ConfigStream);
      continue;
    } catch (...) {
      LOG(Sev::Error, "Unknown error caught while trying to initialise stream  "
                      "parent: {}  json: {}",
          StreamHDFInfo.HDFParentName, StreamHDFInfo.ConfigStream);
    }
  }
  return StreamSettingsList;
}

void CommandHandler::handleNew(std::string const &Command) {
  using nlohmann::json;
  using std::move;
  using std::string;
  json Doc = parseOrThrow(Command);

  std::shared_ptr<KafkaW::ProducerTopic> StatusProducer;
  if (MasterPtr != nullptr) {
    StatusProducer = MasterPtr->getStatusProducer();
  }
  auto Task =
      std::make_unique<FileWriterTask>(Config.service_id, StatusProducer);
  if (auto x = find<std::string>("job_id", Doc)) {
    std::string JobID = x.inner();
    if (JobID.empty()) {
      throwMissingKey("job_id", Doc.dump());
    }
    Task->setJobId(JobID);
  } else {
    throwMissingKey("job_id", Doc.dump());
  }

  if (MasterPtr != nullptr) {
    logEvent(MasterPtr->getStatusProducer(), StatusCode::Start,
             Config.service_id, Task->jobID(), "Start job");
  }

  uri::URI Broker("//localhost:9092");
  if (auto BrokerStringMaybe = find<std::string>("broker", Doc)) {
    auto BrokerString = BrokerStringMaybe.inner();
    if (BrokerString.substr(0, 2) != "//") {
      BrokerString = std::string("//") + BrokerString;
    }
    Broker.parse(BrokerString);
    LOG(Sev::Debug, "Use main broker: {}", Broker.host_port);
  }

  if (auto FileAttributesMaybe = find<nlohmann::json>("file_attributes", Doc)) {
    if (auto FileNameMaybe =
            find<std::string>("file_name", FileAttributesMaybe.inner())) {
      Task->setFilename(Config.hdf_output_prefix, FileNameMaybe.inner());
    } else {
      throwMissingKey("file_attributes.file_name", Doc.dump());
    }
  } else {
    throwMissingKey("file_attributes", Doc.dump());
  }

  bool UseSwmr = true;
  if (auto UseHDFSWMRMaybe = find<bool>("use_hdf_swmr", Doc)) {
    UseSwmr = UseHDFSWMRMaybe.inner();
  }

  // When FileWriterTask::InitialiseHdf() returns, `stream_hdf_info` will
  // contain the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  if (auto NexusStructureMaybe = find<nlohmann::json>("nexus_structure", Doc)) {
    try {
      StreamHDFInfoList =
          initializeHDF(*Task, NexusStructureMaybe.inner().dump(), UseSwmr);
    } catch (std::runtime_error const &E) {
      std::throw_with_nested(std::runtime_error(
          fmt::format("Failed to initializeHDF: {}", E.what())));
    }
  } else {
    throwMissingKey("nexus_structure", Doc.dump());
  }

  std::vector<StreamSettings> StreamSettingsList =
      extractStreamInformationFromJson(Task, StreamHDFInfoList);

  if (auto ThrowOnUninitialisedStreamMaybe =
          find<bool>("abort_on_uninitialised_stream", Doc)) {
    if (ThrowOnUninitialisedStreamMaybe.inner()) {
      for (auto const &Item : StreamHDFInfoList) {
        if (!Item.InitialisedOk) {
          throw std::runtime_error(fmt::format("Could not initialise {}  {}",
                                               Item.HDFParentName,
                                               Item.ConfigStream));
        }
      }
    }
  }

  addStreamSourceToWriterModule(StreamSettingsList, Task);

  // Must be done before StreamMaster instantiation
  if (auto x = find<uint64_t>("start_time", Doc)) {
    std::chrono::milliseconds StartTime(x.inner());
    if (StartTime.count() != 0) {
      LOG(Sev::Info, "StartTime: {}", StartTime.count());
      Config.StreamerConfiguration.StartTimestamp = StartTime;
    }
  }
  if (auto x = find<uint64_t>("stop_time", Doc)) {
    std::chrono::milliseconds StopTime(x.inner());
    if (StopTime.count() != 0) {
      LOG(Sev::Info, "StopTime: {}", StopTime.count());
      Config.StreamerConfiguration.StopTimestamp = StopTime;
    }
  }

  if (MasterPtr != nullptr) {
    // Register the task with master.
    LOG(Sev::Info, "Write file with job_id: {}", Task->jobID());
    auto s = std::make_unique<StreamMaster<Streamer>>(
        Broker.host_port, std::move(Task), Config,
        MasterPtr->getStatusProducer());
    if (auto status_producer = MasterPtr->getStatusProducer()) {
      s->report(std::chrono::milliseconds{Config.status_master_interval});
    }
    if (Config.topic_write_duration.count()) {
      s->TopicWriteDuration = Config.topic_write_duration;
    }
    s->start();

    MasterPtr->addStreamMaster(std::move(s));
  } else {
    FileWriterTasks.emplace_back(std::move(Task));
  }
}

/// \brief Configure the HDF writer modules for writing.
///
/// \param StreamSettingsList The settings for the stream.
/// \param Task The task to configure.
void CommandHandler::addStreamSourceToWriterModule(
    std::vector<StreamSettings> &StreamSettingsList,
    std::unique_ptr<FileWriterTask> &Task) {
  bool UseParallelWriter = false;

  for (auto &StreamSettings : StreamSettingsList) {
    if (!UseParallelWriter || !StreamSettings.RunParallel) {
      LOG(Sev::Debug, "add Source as non-parallel: {}", StreamSettings.Topic);
      HDFWriterModuleRegistry::ModuleFactory ModuleFactory;

      try {
        ModuleFactory = HDFWriterModuleRegistry::find(StreamSettings.Module);
      } catch (std::exception const &E) {
        LOG(Sev::Info, "Module '{}' is not available, error {}",
            StreamSettings.Module, E.what());
        continue;
      }

      auto HDFWriterModule = ModuleFactory();
      if (!HDFWriterModule) {
        LOG(Sev::Info, "Can not create a HDFWriterModule for '{}'",
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
          if (Err.is_ERR()) {
            LOG(Sev::Error, "can not reopen HDF file for stream {}",
                StreamSettings.StreamHDFInfoObj.HDFParentName);
            continue;
          }
        } catch (std::runtime_error const &e) {
          LOG(Sev::Error, "Exception on HDFWriterModule->reopen(): {}",
              e.what());
          continue;
        }

        // Create a Source instance for the stream and add to the task.
        Source ThisSource(StreamSettings.Source, StreamSettings.Module,
                          move(HDFWriterModule));
        ThisSource.setTopic(StreamSettings.Topic);
        Task->addSource(std::move(ThisSource));
      } catch (std::runtime_error const &E) {
        LOG(Sev::Warning,
            "Exception while initializing writer module {} for source {}: {}",
            StreamSettings.Module, StreamSettings.Source, E.what());
        continue;
      }
    }
    StreamSettings.ReopenedOk = true;
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

void CommandHandler::handleStreamMasterStop(std::string const &Command) {
  using std::string;
  LOG(Sev::Debug, "{}", Command);

  nlohmann::json Doc;
  try {
    Doc = nlohmann::json::parse(Command);
  } catch (...) {
    std::throw_with_nested(
        std::runtime_error(fmt::format("Can not parse command: {}", Command)));
  }
  string JobID;
  if (auto x = find<std::string>("job_id", Doc)) {
    JobID = x.inner();
  } else {
    throwMissingKey("job_id", Doc.dump());
  }
  std::chrono::milliseconds StopTime(0);
  if (auto x = find<uint64_t>("stop_time", Doc)) {
    StopTime = std::chrono::milliseconds(x.inner());
  }
  if (MasterPtr != nullptr) {
    auto &StreamMaster = MasterPtr->getStreamMasterForJobID(JobID);
    if (StreamMaster) {
      if (StopTime.count() != 0) {
        LOG(Sev::Info,
            "Received request to gracefully stop file with id : {} at {} ms",
            JobID, StopTime.count());
        StreamMaster->setStopTime(StopTime);
      } else {
        LOG(Sev::Info, "Received request to gracefully stop file with id : {}",
            JobID);
        StreamMaster->stop();
      }
    } else {
      LOG(Sev::Warning, "Can not find StreamMaster for JobID: {}", JobID);
    }
  }
}

void CommandHandler::handle(std::string const &Command) {
  using nlohmann::json;
  json Doc;
  try {
    Doc = json::parse(Command);
  } catch (...) {
    std::throw_with_nested(
        std::runtime_error(fmt::format("Can not parse command: {}", Command)));
  }

  if (auto ServiceIDMaybe = find<std::string>("service_id", Doc)) {
    if (ServiceIDMaybe.inner() != Config.service_id) {
      LOG(Sev::Debug, "Ignoring command addressed to service_id: {}",
          ServiceIDMaybe.inner());
      return;
    }
  }

  uint64_t TeamId = 0;
  uint64_t CommandTeamId = 0;
  if (auto x = find<uint64_t>("teamid", Doc)) {
    CommandTeamId = x.inner();
  }
  if (CommandTeamId != TeamId) {
    LOG(Sev::Info, "INFO command is for teamid {:016x}, we are {:016x}",
        CommandTeamId, TeamId);
    return;
  }

  if (auto CmdMaybe = find<std::string>("cmd", Doc)) {
    std::string CommandMain = CmdMaybe.inner();
    if (CommandMain == "FileWriter_new") {
      handleNew(Command);
      return;
    }
    if (CommandMain == "FileWriter_exit") {
      handleExit();
      return;
    }
    if (CommandMain == "FileWriter_stop") {
      handleStreamMasterStop(Command);
      return;
    }
    if (CommandMain == "file_writer_tasks_clear_all") {
      if (auto y = find<std::string>("recv_type", Doc)) {
        std::string ReceiverType = y.inner();
        if (ReceiverType == "FileWriter") {
          handleFileWriterTaskClearAll();
          return;
        }
      } else {
        throwMissingKey("recv_type", Doc.dump());
      }
    }
  } else {
    LOG(Sev::Warning, "Can not extract 'cmd' from command {}", Command);
  }
  LOG(Sev::Warning, "Could not understand this command: {}", Command);
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

void CommandHandler::tryToHandle(std::string const &Command) {
  try {
    handle(Command);
  } catch (...) {
    std::string JobID;
    try {
      JobID = nlohmann::json::parse(Command)["job_id"];
    } catch (...) {
    }
    try {
      std::throw_with_nested(
          std::runtime_error("Error in CommandHandler::tryToHandle"));
    } catch (std::runtime_error const &E) {
      auto Message = fmt::format(
          "Unexpected std::exception while handling command:\n{}\n{}", Command,
          format_nested_exception(E));
      LOG(Sev::Error, "JobID: {}  StatusCode: {}  Message: {}", JobID,
          convertStatusCodeToString(StatusCode::Fail), Message);
      if (MasterPtr != nullptr) {
        logEvent(MasterPtr->getStatusProducer(), StatusCode::Fail,
                 Config.service_id, JobID, Message);
      }
    }
  }
}

void CommandHandler::tryToHandle(Msg const &Msg) {
  tryToHandle({(char *)Msg.data(), Msg.size()});
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
