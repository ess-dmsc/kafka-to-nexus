#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "helper.h"
#include "json.h"
#include <future>
#include <nlohmann/json.hpp>

using std::array;
using std::vector;

namespace FileWriter {

static nlohmann::json parseOrThrow(std::string const &Command) {
  try {
    return nlohmann::json::parse(Command);
  } catch (nlohmann::detail::parse_error &e) {
    LOG(Sev::Warning, "Can not parse command: {}", Command);
    throw;
  }
}

static void logMissingKey(std::string const &Key, std::string const &Context) {
  LOG(Sev::Warning, "Missing key {} from {}", Key, Context);
}

/// Helper function to extract the broker from the file writer command.

std::string findBroker(std::string const &Command) {
  nlohmann::json Doc = parseOrThrow(Command);
  if (auto x = get<std::string>("broker", Doc)) {
    std::string BrokerHostPort = x.inner();
    if (BrokerHostPort.substr(0, 2) == "//") {
      uri::URI u(BrokerHostPort);
      return u.host_port;
    } else {
      return BrokerHostPort;
    }
  } else {
    logMissingKey("broker", Command);
  }
  return std::string("localhost:9092");
}

// In the future, want to handle many, but not right now.
static int g_N_HANDLED = 0;

CommandHandler::CommandHandler(MainOpt &Config_, Master *MasterPtr_)
    : Config(Config_), MasterPtr(MasterPtr_) {}

// POD

struct StreamSettings {
  StreamHDFInfo StreamHDFInfoObj;
  std::string Topic;
  std::string Module;
  std::string Source;
  bool RunParallel = false;
  std::string ConfigStreamJson;
};

/// \brief Given a task and the `nexus_structure` as json string, set up the
/// basic HDF file structure.

std::vector<StreamHDFInfo>
CommandHandler::initializeHDF(FileWriterTask &Task,
                              std::string const &NexusStructureString) const {
  using nlohmann::json;
  json NexusStructure = json::parse(NexusStructureString);
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  json ConfigFile = json::parse("{}");
  int x = Task.hdf_init(NexusStructure.dump(), ConfigFile.dump(),
                        StreamHDFInfoList);
  if (x) {
    LOG(Sev::Error, "hdf_init failed, cancel this command");
    throw std::runtime_error("");
  }
  return StreamHDFInfoList;
}

/// Extracts the information about the stream from the json command and calls
/// the corresponding HDF writer modules to set u pthe initial HDF structures
/// in the output file.

static std::vector<StreamSettings> extractStreamInformationFromJson(
    std::unique_ptr<FileWriterTask> const &Task,
    std::vector<StreamHDFInfo> const &StreamHDFInfoList) {
  using nlohmann::detail::out_of_range;
  using nlohmann::json;
  LOG(Sev::Info, "Command contains {} streams", StreamHDFInfoList.size());
  std::vector<StreamSettings> StreamSettingsList;
  for (auto const &stream : StreamHDFInfoList) {
    StreamSettings StreamSettings;
    StreamSettings.StreamHDFInfoObj = stream;

    json ConfigStream;
    try {
      ConfigStream = json::parse(stream.config_stream);
    } catch (nlohmann::detail::parse_error const &e) {
      LOG(Sev::Warning, "Invalid json: {}", stream.config_stream);
      continue;
    }

    json ConfigStreamInner;
    if (auto x = get<json>("stream", ConfigStream)) {
      ConfigStreamInner = x.inner();
    } else {
      logMissingKey("stream", ConfigStream.dump());
      continue;
    }

    StreamSettings.ConfigStreamJson = ConfigStreamInner.dump();
    LOG(Sev::Info, "Adding stream: {}", StreamSettings.ConfigStreamJson);

    if (auto x = get<json>("topic", ConfigStreamInner)) {
      StreamSettings.Topic = x.inner();
    } else {
      logMissingKey("topic", ConfigStreamInner.dump());
      continue;
    }

    if (auto x = get<std::string>("source", ConfigStreamInner)) {
      StreamSettings.Source = x.inner();
    } else {
      logMissingKey("source", ConfigStreamInner.dump());
      continue;
    }

    if (auto x = get<std::string>("writer_module", ConfigStreamInner)) {
      StreamSettings.Module = x.inner();
    } else {
      logMissingKey("writer_module", ConfigStreamInner.dump());
      // Allow the old key name as well:
      if (auto x = get<std::string>("module", ConfigStreamInner)) {
        StreamSettings.Module = x.inner();
        LOG(Sev::Notice, "The key \"stream.module\" is deprecated, please use "
                         "\"stream.writer_module\" instead.");
      } else {
        logMissingKey("module", ConfigStreamInner.dump());
        continue;
      }
    }

    if (auto x = get<bool>("run_parallel", ConfigStream)) {
      StreamSettings.RunParallel = x.inner();
    }
    if (StreamSettings.RunParallel) {
      LOG(Sev::Info, "Run parallel for source: {}", StreamSettings.Source);
    }

    StreamSettingsList.push_back(StreamSettings);

    auto ModuleFactory = HDFWriterModuleRegistry::find(StreamSettings.Module);
    if (!ModuleFactory) {
      LOG(Sev::Warning, "Module '{}' is not available", StreamSettings.Module);
      continue;
    }

    auto HDFWriterModule = ModuleFactory();
    if (!HDFWriterModule) {
      LOG(Sev::Warning, "Can not create a HDFWriterModule for '{}'",
          StreamSettings.Module);
      continue;
    }

    auto RootGroup = Task->hdf_file.h5file.root();
    auto ConfigStreamRapidjson =
        stringToRapidjsonOrThrow(ConfigStreamInner.dump());
    HDFWriterModule->parse_config(ConfigStreamRapidjson, nullptr);
    CollectiveQueue *cq = nullptr;
    rapidjson::Document AttributesDocument;
    if (auto x = get<json>("attributes", ConfigStream)) {
      AttributesDocument = stringToRapidjsonOrThrow(x.inner().dump());
    }
    rapidjson::Value const *AttributesPtr = nullptr;
    if (AttributesDocument.IsObject()) {
      AttributesPtr = &AttributesDocument;
    }
    HDFWriterModule->init_hdf(RootGroup, stream.hdf_parent_name, AttributesPtr,
                              cq);
    HDFWriterModule->close();
    HDFWriterModule.reset();
  }
  return StreamSettingsList;
}

/// \brief Given a JSON string, create a new file writer job.
///
/// Creates a new `FileWriterTask`, sets information such as file name, job id.
/// Goes on and calls `initializeHDF` to initialize the basic HDF group
/// structure in the output file. It then extracts the information about the
/// data streams by calling `extractStreamInformationFromJson`. The HDF file is
/// closed and re-opened to optionally support SWMR and parallel writing. In a
/// second pass, it calls `addStreamSourceToWriterModule` to instantiate
/// `Source` objects which in turn re-open the HDF datasets for writing.
/// Finally, we register the `FileWriterTask` with `Master`.

void CommandHandler::handleNew(std::string const &Command) {
  using std::move;
  using std::string;
  using nlohmann::detail::out_of_range;
  using nlohmann::json;
  json Doc = parseOrThrow(Command);

  auto Task = std::unique_ptr<FileWriterTask>(new FileWriterTask);
  if (auto x = get<std::string>("job_id", Doc)) {
    std::string JobID = x.inner();
    if (JobID.empty()) {
      logMissingKey("job_id", Doc.dump());
      return;
    }
    Task->job_id_init(JobID);
  } else {
    logMissingKey("job_id", Doc.dump());
    return;
  }

  if (auto y = get<nlohmann::json>("file_attributes", Doc)) {
    if (auto x = get<std::string>("file_name", y.inner())) {
      Task->set_hdf_filename(Config.hdf_output_prefix, x.inner());
    } else {
      logMissingKey("file_attributes.file_name", Doc.dump());
      return;
    }
  } else {
    logMissingKey("file_attributes", Doc.dump());
    return;
  }

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  if (auto x = get<nlohmann::json>("nexus_structure", Doc)) {
    try {
      StreamHDFInfoList = initializeHDF(*Task, x.inner().dump());
    } catch (std::runtime_error const &e) {
      LOG(Sev::Error, "Failed to initializeHDF: {}", e.what());
    }
  } else {
    logMissingKey("nexus_structure", Doc.dump());
    return;
  }

  std::vector<StreamSettings> StreamSettingsList =
      extractStreamInformationFromJson(Task, StreamHDFInfoList);

  Task->hdf_close();
  Task->hdf_reopen();

  addStreamSourceToWriterModule(StreamSettingsList, Task);

  // Must be done before StreamMaster instantiation
  if (auto x = get<uint64_t>("start_time", Doc)) {
    std::chrono::milliseconds StartTime(x.inner());
    if (StartTime.count() != 0) {
      LOG(Sev::Info, "StartTime: {}", StartTime.count());
      Config.StreamerConfiguration.StartTimestamp = StartTime;
    }
  }

  std::chrono::milliseconds StopTime(0);
  if (auto x = get<uint64_t>("stop_time", Doc)) {
    StopTime = std::chrono::milliseconds(x.inner());
  }

  if (MasterPtr) {
    std::string br = findBroker(Command);

    LOG(Sev::Info, "Write file with job_id: {}", Task->job_id());
    auto s = std::unique_ptr<StreamMaster<Streamer>>(new StreamMaster<Streamer>(
        br, std::move(Task), Config.StreamerConfiguration));
    if (MasterPtr->status_producer) {
      s->report(MasterPtr->status_producer,
                std::chrono::milliseconds{Config.status_master_interval});
    }
    if (Config.topic_write_duration.count()) {
      s->TopicWriteDuration = Config.topic_write_duration;
    }
    s->start();

    if (StopTime.count() != 0) {
      LOG(Sev::Info, "StopTime: {}", StopTime.count());
      s->setStopTime(StopTime);
    }

    MasterPtr->stream_masters.push_back(std::move(s));
  } else {
    FileWriterTasks.emplace_back(std::move(Task));
  }
  g_N_HANDLED += 1;
}

/// \brief Given a `FileWriterTask` and a list of `StreamSettings`, it sets up
/// the HDF writer modules for writing.
///
/// It creates the `HDFWriterModule` instances which in turn re-open the
/// previously created HDF datasets. It creates then a `Source` instance for
/// each stream and adds those to the `FileWriterTask`.

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

      rapidjson::Document ConfigStream;
      ConfigStream.Parse(StreamSettings.ConfigStreamJson.c_str());
      HDFWriterModule->parse_config(ConfigStream, nullptr);
      try {
        auto RootGroup = Task->hdf_file.h5file.root();
        auto Err = HDFWriterModule->reopen(
            RootGroup, StreamSettings.StreamHDFInfoObj.hdf_parent_name, nullptr,
            nullptr);
        if (Err.is_ERR()) {
          LOG(Sev::Error, "can not reopen HDF file for stream {}",
              StreamSettings.StreamHDFInfoObj.hdf_parent_name);
          continue;
        }
      } catch (std::runtime_error const &e) {
        LOG(Sev::Error, "Exception on HDFWriterModule->reopen(): {}", e.what());
        continue;
      }

      Source ThisSource(StreamSettings.Source, move(HDFWriterModule));
      ThisSource._topic = std::string(StreamSettings.Topic);
      ThisSource.do_process_message = Config.source_do_process_message;
      Task->add_source(std::move(ThisSource));
    }
  }
}

/// Stop and remove all ongoing file writer jobs.

void CommandHandler::handleFileWriterTaskClearAll() {
  if (MasterPtr) {
    for (auto &x : MasterPtr->stream_masters) {
      x->stop();
    }
  }
  FileWriterTasks.clear();
}

/// Stop the whole file writer application.

void CommandHandler::handleExit() {
  if (MasterPtr) {
    MasterPtr->stop();
  }
}

/// Stops a given job

void CommandHandler::handleStreamMasterStop(std::string const &Command) {
  using std::string;
  if (!MasterPtr) {
    return;
  }
  nlohmann::json Doc;
  try {
    Doc = nlohmann::json::parse(Command);
  } catch (...) {
    LOG(Sev::Warning, "Can not parse command: {}", Command);
    return;
  }
  string JobID;
  if (auto x = get<std::string>("job_id", Doc)) {
    JobID = x.inner();
  } else {
    logMissingKey("job_id", Doc.dump());
    return;
  }
  std::chrono::milliseconds StopTime(0);
  if (auto x = get<uint64_t>("stop_time", Doc)) {
    StopTime = std::chrono::milliseconds(x.inner());
  }
  int counter{0};
  for (auto &x : MasterPtr->stream_masters) {
    if (x->getJobId() == JobID) {
      if (StopTime.count() != 0) {
        LOG(Sev::Info, "gracefully stop file with id : {} at {} ms", JobID,
            StopTime.count());
        x->setStopTime(StopTime);
      } else {
        LOG(Sev::Info, "gracefully stop file with id : {}", JobID);
        x->stop();
      }
      ++counter;
    }
  }
  if (counter == 0) {
    LOG(Sev::Warning, "no file with id : {}", JobID);
  } else if (counter > 1) {
    LOG(Sev::Warning, "error: multiple files with id : {}", JobID);
  }
}

/// Parses the given command and passes it on to a more specific handler.

void CommandHandler::handle(std::string const &Command) {
  using nlohmann::json;
  json Doc;
  try {
    Doc = json::parse(Command);
  } catch (...) {
    LOG(Sev::Error, "Can not parse json command: {}", Command);
    return;
  }
  uint64_t TeamId = 0;
  uint64_t CommandTeamId = 0;
  if (MasterPtr) {
    TeamId = MasterPtr->config.teamid;
  }
  if (auto x = get<uint64_t>("teamid", Doc)) {
    CommandTeamId = x.inner();
  }
  if (CommandTeamId != TeamId) {
    LOG(Sev::Info, "INFO command is for teamid {:016x}, we are {:016x}",
        CommandTeamId, TeamId);
    return;
  }

  if (auto x = get<std::string>("cmd", Doc)) {
    std::string CommandMain = x.inner();
    if (CommandMain == "FileWriter_new") {
      handleNew(Command);
      return;
    }
    if (CommandMain == "FileWriter_exit") {
      handleExit();
      return;
    }
    if (CommandMain == "FileWriter_stop") {
      handleStreamMasterStop(Doc);
      return;
    }
    if (CommandMain == "file_writer_tasks_clear_all") {
      if (auto y = get<std::string>("recv_type", Doc)) {
        std::string ReceiverType = y.inner();
        if (ReceiverType == "FileWriter") {
          handleFileWriterTaskClearAll();
          return;
        }
      } else {
        logMissingKey("recv_type", Doc.dump());
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
  } catch (nlohmann::detail::parse_error &e) {
    LOG(Sev::Error, "parse_error: {}  Command: {}", e.what(), Command);
  } catch (nlohmann::detail::out_of_range &e) {
    LOG(Sev::Error, "out_of_range: {}  Command: ", e.what(), Command);
  } catch (nlohmann::detail::type_error &e) {
    LOG(Sev::Error, "type_error: {}  Command: ", e.what(), Command);
  } catch (...) {
    LOG(Sev::Error, "Unexpected error while handling command: {}", Command);
    throw;
  }
}

/// Given a `Msg`, call `CommandHandler::handle(std::string const &Command)`.

void CommandHandler::handle(Msg const &Msg) {
  tryToHandle({(char *)Msg.data(), Msg.size()});
}

} // namespace FileWriter
