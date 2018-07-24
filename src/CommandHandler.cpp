#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "StreamMaster.h"
#include "Streamer.h"
#include "helper.h"
#include "json.h"
#include <chrono>
#include <future>

using std::array;
using std::vector;

namespace FileWriter {

using nlohmann::json;

static json parseOrThrow(std::string const &Command) {
  try {
    return json::parse(Command);
  } catch (json::parse_error const &E) {
    LOG(Sev::Warning, "Can not parse command  what: {}  Command: {}", E.what(),
        Command);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Can not parse command  what: {}  Command: {}", E.what(), Command)));
  }
}

static void throwMissingKey(std::string const &Key,
                            std::string const &Context) {
  throw std::runtime_error(fmt::format("Missing key {} from {}", Key, Context));
}

std::chrono::milliseconds findTime(nlohmann::json const &Doc,
                                   std::string const &TimeName) {
  if (auto x = find<uint64_t>(TimeName, Doc)) {
    std::chrono::milliseconds Time(x.inner());
    if (Time.count() != 0) {
      return Time;
    }
  }
  return std::chrono::milliseconds{-1};
}

// In the future, want to handle many, but not right now.
static int g_N_HANDLED = 0;

CommandHandler::CommandHandler(MainOpt &Config_, MasterI *MasterPtr_)
    : Config(Config_), MasterPtr(MasterPtr_),
      KafkaMsgTimestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())) {}

/// Holder for the stream settings.
struct StreamSettings {
  StreamHDFInfo StreamHDFInfoObj;
  std::string Topic;
  std::string Module;
  std::string Source;
  bool RunParallel = false;
  std::string ConfigStreamJson;
};

std::vector<StreamHDFInfo>
CommandHandler::initializeHDF(FileWriterTask &Task,
                              std::string const &NexusStructureString) const {
  using nlohmann::json;
  json NexusStructure = json::parse(NexusStructureString);
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  json ConfigFile = json::parse("{}");
  Task.hdf_init(NexusStructure.dump(), ConfigFile.dump(), StreamHDFInfoList);
  return StreamHDFInfoList;
}

/// Extracts the information about the stream from the json command and calls
/// the corresponding HDF writer modules to set up the initial HDF structures
/// in the output file.
///
/// \param Task The task which will write the HDF file.
/// \param StreamHDFInfoList
/// \return
static StreamSettings extractStreamInformationFromJsonForSource(
    std::unique_ptr<FileWriterTask> const &Task,
    StreamHDFInfo const &StreamHDFInfo) {
  using nlohmann::json;
  StreamSettings StreamSettings;
  StreamSettings.StreamHDFInfoObj = StreamHDFInfo;

  json ConfigStream;
  ConfigStream = json::parse(StreamHDFInfo.config_stream);

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

  auto ModuleFactory = HDFWriterModuleRegistry::find(StreamSettings.Module);
  if (!ModuleFactory) {
    throw std::runtime_error(
        fmt::format("Module '{}' is not available", StreamSettings.Module));
  }

  auto HDFWriterModule = ModuleFactory();
  if (!HDFWriterModule) {
    throw std::runtime_error(fmt::format(
        "Can not create a HDFWriterModule for '{}'", StreamSettings.Module));
  }

  auto RootGroup = Task->hdf_file.h5file.root();
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
      hdf5::node::get_group(RootGroup, StreamHDFInfo.hdf_parent_name);
  HDFWriterModule->init_hdf({StreamGroup}, Attributes.dump());
  HDFWriterModule->close();
  HDFWriterModule.reset();
  return StreamSettings;
}

static std::vector<StreamSettings> extractStreamInformationFromJson(
    std::unique_ptr<FileWriterTask> const &Task,
    std::vector<StreamHDFInfo> const &StreamHDFInfoList) {
  LOG(Sev::Info, "Command contains {} streams", StreamHDFInfoList.size());
  std::vector<StreamSettings> StreamSettingsList;
  for (auto const &StreamHDFInfo : StreamHDFInfoList) {
    try {
      StreamSettingsList.push_back(
          extractStreamInformationFromJsonForSource(Task, StreamHDFInfo));
    } catch (json::parse_error const &E) {
      LOG(Sev::Warning, "Invalid json: {}", StreamHDFInfo.config_stream);
      continue;
    } catch (std::runtime_error const &E) {
      LOG(Sev::Warning,
          "Exception while initializing writer module  what: {}  json: {}",
          E.what(), StreamHDFInfo.config_stream);
      continue;
    }
  }
  return StreamSettingsList;
}

void CommandHandler::handleNew(std::string const &Command) {
  using nlohmann::json;
  using std::move;
  using std::string;
  json Doc = parseOrThrow(Command);

  auto Task = std::unique_ptr<FileWriterTask>(new FileWriterTask);
  if (auto x = find<std::string>("job_id", Doc)) {
    std::string JobID = x.inner();
    if (JobID.empty()) {
      throwMissingKey("job_id", Doc.dump());
    }
    Task->job_id_init(JobID);
  } else {
    throwMissingKey("job_id", Doc.dump());
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
      Task->set_hdf_filename(Config.hdf_output_prefix, FileNameMaybe.inner());
    } else {
      throwMissingKey("file_attributes.file_name", Doc.dump());
    }
  } else {
    throwMissingKey("file_attributes", Doc.dump());
  }

  if (auto UseHDFSWMRMaybe = find<bool>("use_hdf_swmr", Doc)) {
    Task->UseHDFSWMR = UseHDFSWMRMaybe.inner();
  }

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  if (auto NexusStructureMaybe = find<nlohmann::json>("nexus_structure", Doc)) {
    try {
      StreamHDFInfoList =
          initializeHDF(*Task, NexusStructureMaybe.inner().dump());
    } catch (std::runtime_error const &E) {
      std::throw_with_nested(std::runtime_error(
          fmt::format("Failed to initializeHDF: {}", E.what())));
    }
  } else {
    throwMissingKey("nexus_structure", Doc.dump());
  }

  std::vector<StreamSettings> StreamSettingsList =
      extractStreamInformationFromJson(Task, StreamHDFInfoList);

  // The HDF file is closed and re-opened to (optionally) support SWMR and
  // parallel writing.
  Task->hdf_close();
  Task->hdf_reopen();

  addStreamSourceToWriterModule(StreamSettingsList, Task);

  std::chrono::milliseconds Time = findTime(Doc, "start_time");
  if (Time.count() > 0) {
    Config.StreamerConfiguration.StartTimestamp = Time;
  }
  LOG(Sev::Info, "Start time: {}",
      Config.StreamerConfiguration.StartTimestamp.count());
  Time = findTime(Doc, "stop_time");
  if (Time.count() > 0) {
    Config.StreamerConfiguration.StopTimestamp = Time;
  }

  if (MasterPtr) {
    // Register the task with master.
    LOG(Sev::Info, "Write file with job_id: {}", Task->job_id());
    auto s = std::unique_ptr<StreamMaster<Streamer>>(new StreamMaster<Streamer>(
        Broker.host_port, std::move(Task), Config.StreamerConfiguration));
    if (auto status_producer = MasterPtr->getStatusProducer()) {
      s->report(status_producer,
                std::chrono::milliseconds{Config.status_master_interval});
    }
    if (Config.topic_write_duration.count()) {
      s->TopicWriteDuration = Config.topic_write_duration;
    }
    s->start();

    MasterPtr->addStreamMaster(std::move(s));
  } else {
    FileWriterTasks.emplace_back(std::move(Task));
  }
  g_N_HANDLED += 1;
}

void CommandHandler::addStreamSourceToWriterModule(
    const std::vector<StreamSettings> &StreamSettingsList,
    std::unique_ptr<FileWriterTask> &Task) {
  bool UseParallelWriter = false;

  for (auto const &StreamSettings : StreamSettingsList) {
    if (UseParallelWriter && StreamSettings.RunParallel) {
    } else {
      LOG(Sev::Debug, "add Source as non-parallel: {}", StreamSettings.Topic);
      auto ModuleFactory = HDFWriterModuleRegistry::find(StreamSettings.Module);
      if (!ModuleFactory) {
        LOG(Sev::Info, "Module '{}' is not available", StreamSettings.Module);
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
          auto RootGroup = Task->hdf_file.h5file.root();
          auto StreamGroup = hdf5::node::get_group(
              RootGroup, StreamSettings.StreamHDFInfoObj.hdf_parent_name);
          auto Err = HDFWriterModule->reopen({StreamGroup});
          if (Err.is_ERR()) {
            LOG(Sev::Error, "can not reopen HDF file for stream {}",
                StreamSettings.StreamHDFInfoObj.hdf_parent_name);
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
        ThisSource._topic = std::string(StreamSettings.Topic);
        ThisSource.do_process_message = Config.source_do_process_message;
        Task->add_source(std::move(ThisSource));
      } catch (std::runtime_error const &E) {
        LOG(Sev::Warning,
            "Exception while initializing writer module {} for source {}: {}",
            StreamSettings.Module, StreamSettings.Source, E.what());
        continue;
      }
    }
  }
}

void CommandHandler::handleFileWriterTaskClearAll() {
  if (MasterPtr) {
    MasterPtr->stopStreamMasters();
  }
  FileWriterTasks.clear();
}

void CommandHandler::handleExit() {
  if (MasterPtr) {
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
  std::chrono::milliseconds StopTime = findTime(Doc, "stop_time");
  if (MasterPtr) {
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
  } else {
    // Currently, we interpret commands which have no service_id.
    // In the future, we may want to ignore all commands which are not
    // specifically addressed to us (breaking change).
    // In that case, just uncomment the following return:
    // return;
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

void CommandHandler::tryToHandle(std::string const &Command) {
  try {
    handle(Command);
  } catch (json::parse_error const &E) {
    LOG(Sev::Error, "parse_error: {}  Command: {}", E.what(), Command);
  } catch (json::out_of_range const &E) {
    LOG(Sev::Error, "out_of_range: {}  Command: ", E.what(), Command);
  } catch (json::type_error const &E) {
    LOG(Sev::Error, "type_error: {}  Command: ", E.what(), Command);
  } catch (std::runtime_error const &E) {
    // Originates from h5cpp:
    if (std::string(E.what()).find(
            "Cannot obtain ObjectId from an invalid file instance!") == 0) {
      LOG(Sev::Warning, "Exception while creating HDF output file, maybe "
                        "output file already exists.  command: {}",
          Command);
    } else {
      LOG(Sev::Error, "Unexpected std::runtime_error.  what: {}  command: {}",
          E.what(), Command);
    }
  } catch (std::exception const &E) {
    LOG(Sev::Error, "Unexpected std::exception while handling command: {}",
        Command);
    std::throw_with_nested(std::runtime_error(
        fmt::format("Unexpected std::exception while handling command  what: "
                    "{}  Command: {}",
                    E.what(), Command)));
  } catch (...) {
    LOG(Sev::Error, "Unexpected unknown exception while handling command: {}",
        Command);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Unexpected unknown exception while handling command  Command: {}",
        Command)));
  }
}

void CommandHandler::handle(Msg const &Msg, int64_t MsgTimestamp) {
  // if kafka message timestamp is invalid (e.g. old broker) use current time
  if (MsgTimestamp > 0) {
    KafkaMsgTimestamp = std::chrono::milliseconds{MsgTimestamp};
  } else {
    LOG(Sev::Warning, "Invalid KafkaMsgTimestamp, using current time",
        MsgTimestamp);
  }
  tryToHandle({(char *)Msg.data(), Msg.size()});
}

size_t CommandHandler::getNumberOfFileWriterTasks() const {
  return FileWriterTasks.size();
}

std::unique_ptr<FileWriterTask> &
CommandHandler::getFileWriterTaskByJobID(std::string JobID) {
  for (auto &Task : FileWriterTasks) {
    if (Task->job_id() == JobID) {
      return Task;
    }
  }
  static std::unique_ptr<FileWriterTask> NotFound;
  return NotFound;
}

} // namespace FileWriter
