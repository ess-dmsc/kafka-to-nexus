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

/// Helper function to extract the broker from the file writer command.

std::string findBroker(std::string const &Command) {
  nlohmann::json Doc = parseOrThrow(Command);
  try {
    std::string BrokerHostPort = Doc.at("broker");
    if (BrokerHostPort.substr(0, 2) == "//") {
      uri::URI u(BrokerHostPort);
      return u.host_port;
    } else {
      return BrokerHostPort;
    }
  } catch (...) {
    LOG(Sev::Warning, "Can not find field 'broker' in command: {}", Command);
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

void CommandHandler::handleNew(std::string const &Command) {
  using std::move;
  using std::string;
  using nlohmann::detail::out_of_range;
  using nlohmann::json;
  json Doc = parseOrThrow(Command);

  auto Task = std::unique_ptr<FileWriterTask>(new FileWriterTask);
  try {
    std::string JobID = Doc.at("job_id");
    if (JobID.empty()) {
      LOG(Sev::Warning, "Command not accepted: empty job_id");
      return;
    }
    Task->job_id_init(JobID);
  } catch (out_of_range const &e) {
    LOG(Sev::Warning, "Command not accepted: missing job_id");
    return;
  }

  try {
    Task->set_hdf_filename(Config.hdf_output_prefix, Doc.at("file_attributes").at("file_name"));
  } catch (out_of_range const &e) {
    LOG(Sev::Warning, "Command not accepted: missing file_attributes.file_name");
    return;
  }

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> StreamHDFInfoList;
  {
    json ConfigFile = json::parse("{}");
    json NexusStructure = Doc.at("nexus_structure");
    int x = Task->hdf_init(NexusStructure.dump(), ConfigFile.dump(),
                           StreamHDFInfoList);
    if (x) {
      LOG(Sev::Error, "ERROR hdf init failed, cancel this write command");
      return;
    }
  }

  // Extract some information from the JSON first
  std::vector<StreamSettings> StreamSettingsList;
  LOG(Sev::Info, "Command contains {} streams", StreamHDFInfoList.size());
  for (auto &stream : StreamHDFInfoList) {
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
    try {
      ConfigStreamInner = ConfigStream.at("stream");
    } catch (out_of_range const &e) {
      LOG(Sev::Notice, "Missing stream specification");
      continue;
    }

    StreamSettings.ConfigStreamJson = ConfigStreamInner.dump();
    LOG(Sev::Info, "Adding stream: {}", StreamSettings.ConfigStreamJson);

    try {
      StreamSettings.Topic = ConfigStreamInner.at("topic");
    } catch (out_of_range const &e) {
      LOG(Sev::Notice, "Missing topic on stream specification");
      continue;
    }

    try {
      StreamSettings.Source = ConfigStreamInner.at("source");
    } catch (out_of_range const &e) {
      LOG(Sev::Notice, "Missing source on stream specification");
      continue;
    }

    try {
      try {
        StreamSettings.Module = ConfigStreamInner.at("writer_module");
      } catch (out_of_range const &e) {
        // Allow the old key name as well:
        StreamSettings.Module = ConfigStreamInner.at("module");
        LOG(Sev::Notice, "The key \"stream.module\" is deprecated, please use "
                         "\"stream.writer_module\" instead.");
      }
    } catch (out_of_range const &e) {
      LOG(Sev::Notice, "Missing key `writer_module` on stream specification");
      continue;
    }

    try {
      StreamSettings.RunParallel = ConfigStream.at("run_parallel");
    } catch (out_of_range const &e) {
      // do nothing
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
    try {
      AttributesDocument =
          stringToRapidjsonOrThrow(ConfigStream.at("attributes").dump());
    } catch (out_of_range const &e) {
      // it's ok
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

  Task->hdf_close();
  Task->hdf_reopen();

  addStreamSourceToWriterModule(StreamSettingsList, Task);

  if (MasterPtr) {
    std::string br = findBroker(Command);
    // Must be called before StreamMaster instantiation
    std::chrono::milliseconds StartTime(Doc.at("start_time").get<uint64_t>());
    if (StartTime.count() != 0) {
      LOG(Sev::Info, "StartTime: {}", StartTime.count());
      Config.StreamerConfiguration.StartTimestamp = StartTime;
    }

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

    std::chrono::milliseconds StopTime(Doc.at("stop_time").get<uint64_t>());
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
      auto Err = HDFWriterModule->reopen(
          static_cast<hid_t>(Task->hdf_file.h5file),
          StreamSettings.StreamHDFInfoObj.hdf_parent_name, nullptr, nullptr);
      if (Err.is_ERR()) {
        LOG(Sev::Error, "can not reopen HDF file for stream {}",
            StreamSettings.StreamHDFInfoObj.hdf_parent_name);
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
  try {
    JobID = Doc.at("job_id");
  } catch (...) {
    LOG(Sev::Warning, "File write stop message lacks job_id");
    return;
  }
  std::chrono::milliseconds StopTime(0);
  try {
    StopTime = std::chrono::milliseconds(Doc.at("stop_time"));
  } catch (...) {
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
  try {
    CommandTeamId = Doc.at("teamid").get<int64_t>();
  } catch (...) {
    // do nothing
  }
  if (CommandTeamId != TeamId) {
    LOG(Sev::Info, "INFO command is for teamid {:016x}, we are {:016x}",
        CommandTeamId, TeamId);
    return;
  }

  try {
    std::string CommandMain = Doc.at("cmd");
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
      try {
        std::string ReceiverType = Doc.at("recv_type");
        if (ReceiverType == "FileWriter") {
          handleFileWriterTaskClearAll();
          return;
        }
      } catch (...) {
        LOG(Sev::Warning, "Can not extract 'recv_type' from command {}",
            Command);
      }
    }
  } catch (...) {
    LOG(Sev::Warning, "Can not extract 'cmd' from command {}", Command);
  }
  LOG(Sev::Warning, "Could not understand this command: {}", Command);
}

void CommandHandler::handle(Msg const &Msg) {
  handle({(char *)Msg.data(), Msg.size()});
}

} // namespace FileWriter
