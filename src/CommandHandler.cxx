#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "helper.h"
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
    std::string broker = Doc.at("broker");
    if (broker.substr(0, 2) == "//") {
      uri::URI u(broker);
      return u.host_port;
    } else {
      return broker;
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
  StreamHDFInfo stream_hdf_info;
  std::string topic;
  std::string module;
  std::string source;
  bool run_parallel = false;
  std::string config_stream;
};

void CommandHandler::handleNew(std::string const &Command) {
  using std::move;
  using std::string;
  using nlohmann::detail::out_of_range;
  using nlohmann::json;
  json Doc = parseOrThrow(Command);

  auto fwt = std::unique_ptr<FileWriterTask>(new FileWriterTask);

  string job_id;
  try {
    job_id = Doc.at("job_id");
  } catch (out_of_range const &e) {
  }
  if (job_id.empty()) {
    LOG(Sev::Warning, "Command not accepted: missing job_id");
    return;
  } else {
    fwt->job_id_init(job_id);
  }

  string fname;
  try {
    fname = Doc.at("file_attributes").at("file_name");
  } catch (out_of_range const &e) {
    fname = "a-dummy-name.h5";
  }

  fwt->set_hdf_filename(Config.hdf_output_prefix, fname);

  rapidjson::Document d;
  d.Parse(Command.c_str());

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> stream_hdf_info;
  {
    json ConfigFile = json::parse("{}");
    json NexusStructure = Doc.at("nexus_structure");
    int x = fwt->hdf_init(NexusStructure.dump(), ConfigFile.dump(),
                          stream_hdf_info);
    if (x) {
      LOG(Sev::Error, "ERROR hdf init failed, cancel this write command");
      return;
    }
  }

  // Extract some information from the JSON first
  std::vector<StreamSettings> stream_settings_list;
  LOG(Sev::Info, "Command contains {} streams", stream_hdf_info.size());
  for (auto &stream : stream_hdf_info) {
    StreamSettings stream_settings;
    stream_settings.stream_hdf_info = stream;

    auto config_stream_value = get_object(*stream.config_stream, "stream");
    auto attributes = get_object(*stream.config_stream, "attributes");
    stream_settings.config_stream = json_to_string(*config_stream_value.v);
    LOG(Sev::Info, "Adding stream: {}", stream_settings.config_stream);
    auto &config_stream = *config_stream_value.v;
    auto topic = get_string(&config_stream, "topic");
    if (!topic) {
      LOG(Sev::Notice, "Missing topic on stream specification");
      continue;
    }
    stream_settings.topic = topic.v;
    auto source = get_string(&config_stream, "source");
    if (!source) {
      LOG(Sev::Notice, "Missing source on stream specification");
      continue;
    }
    stream_settings.source = source.v;
    auto module = get_string(&config_stream, "writer_module");
    if (!module) {
      module = get_string(&config_stream, "module");
      if (module) {
        LOG(Sev::Notice, "The key \"stream.module\" is deprecated, please use "
                         "\"stream.writer_module\" instead.");
      } else {
        LOG(Sev::Notice, "Missing key `writer_module` on stream specification");
        continue;
      }
    }
    stream_settings.module = module.v;
    bool run_parallel = false;
    auto run_parallel_cfg = get_bool(&config_stream, "run_parallel");
    if (run_parallel_cfg) {
      run_parallel = run_parallel_cfg.v;
      if (run_parallel) {
        LOG(Sev::Info, "Run parallel {}", source.v);
      }
    }
    stream_settings.run_parallel = run_parallel;

    stream_settings_list.push_back(stream_settings);

    auto module_factory = HDFWriterModuleRegistry::find(module.v);
    if (!module_factory) {
      LOG(Sev::Warning, "Module '{}' is not available", module.v);
      continue;
    }

    auto hdf_writer_module = module_factory();
    if (!hdf_writer_module) {
      LOG(Sev::Warning, "Can not create a HDFWriterModule for '{}'", module.v);
      continue;
    }

    auto root_group = fwt->hdf_file.h5file.root();
    hdf_writer_module->parse_config(config_stream, nullptr);
    CollectiveQueue *cq = nullptr;
    hdf_writer_module->init_hdf(root_group, stream.hdf_parent_name,
                                attributes.v, cq);
    hdf_writer_module->close();
    hdf_writer_module.reset();
  }

  fwt->hdf_close();
  fwt->hdf_reopen();

  addStreamSourceToWriterModule(stream_settings_list, fwt);

  if (MasterPtr) {
    std::string br = findBroker(Command);
    // Must be called before StreamMaster instantiation
    std::chrono::milliseconds StartTime(Doc.at("start_time").get<uint64_t>());
    if (StartTime.count() != 0) {
      LOG(Sev::Info, "StartTime: {}", StartTime.count());
      Config.StreamerConfiguration.StartTimestamp = StartTime;
    }

    LOG(Sev::Info, "Write file with job_id: {}", job_id);
    auto s = std::unique_ptr<StreamMaster<Streamer>>(new StreamMaster<Streamer>(
        br, std::move(fwt), Config.StreamerConfiguration));
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
    FileWriterTasks.emplace_back(std::move(fwt));
  }
  g_N_HANDLED += 1;
}

void CommandHandler::addStreamSourceToWriterModule(
    const std::vector<StreamSettings> &stream_settings_list,
    std::unique_ptr<FileWriterTask> &fwt) {
  bool use_parallel_writer = false;

  for (const auto &stream_settings : stream_settings_list) {
    if (use_parallel_writer && stream_settings.run_parallel) {
    } else {
      LOG(Sev::Debug, "add Source as non-parallel: {}", stream_settings.topic);
      auto module_factory =
          HDFWriterModuleRegistry::find(stream_settings.module);
      if (!module_factory) {
        LOG(Sev::Info, "Module '{}' is not available", stream_settings.module);
        continue;
      }

      auto hdf_writer_module = module_factory();
      if (!hdf_writer_module) {
        LOG(Sev::Info, "Can not create a HDFWriterModule for '{}'",
            stream_settings.module);
        continue;
      }

      rapidjson::Document config_stream;
      config_stream.Parse(stream_settings.config_stream.c_str());
      hdf_writer_module->parse_config(config_stream, nullptr);
      auto err = hdf_writer_module->reopen(
          static_cast<hid_t>(fwt->hdf_file.h5file),
          stream_settings.stream_hdf_info.hdf_parent_name, nullptr, nullptr);
      if (err.is_ERR()) {
        LOG(Sev::Error, "can not reopen HDF file for stream {}",
            stream_settings.stream_hdf_info.hdf_parent_name);
        exit(1);
      }

      auto s = Source(stream_settings.source, move(hdf_writer_module));
      s._topic = std::string(stream_settings.topic);
      s.do_process_message = Config.source_do_process_message;
      fwt->add_source(std::move(s));
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
  string job_id;
  try {
    job_id = Doc.at("job_id");
  } catch (...) {
    LOG(Sev::Warning, "File write stop message lacks job_id");
    return;
  }
  std::chrono::milliseconds stop_time(0);
  try {
    stop_time = std::chrono::milliseconds(Doc.at("stop_time"));
  } catch (...) {
  }
  int counter{0};
  for (auto &x : MasterPtr->stream_masters) {
    if (x->getJobId() == job_id) {
      if (stop_time.count()) {
        LOG(Sev::Info, "gracefully stop file with id : {} at {} ms", job_id,
            stop_time.count());
        x->setStopTime(stop_time);
      } else {
        LOG(Sev::Info, "gracefully stop file with id : {}", job_id);
        x->stop();
      }
      ++counter;
    }
  }
  if (counter == 0) {
    LOG(Sev::Warning, "no file with id : {}", job_id);
  } else if (counter > 1) {
    LOG(Sev::Warning, "error: multiple files with id : {}", job_id);
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

void CommandHandler::handle(Msg const &msg) {
  handle({(char *)msg.data(), msg.size()});
}

} // namespace FileWriter
