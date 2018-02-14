#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "helper.h"
#include <future>
#include <nlohmann/json.hpp>

using std::array;
using std::vector;

namespace FileWriter {

std::string find_broker(rapidjson::Document const &d) {
  auto m = d.FindMember("broker");
  if (m != d.MemberEnd() && m->value.IsString()) {
    auto s = std::string(m->value.GetString());
    if (s.substr(0, 2) == "//") {
      uri::URI u(s);
      return u.host_port;
    } else {
      return s;
    }
  }
  return std::string{"localhost:9092"};
}

std::chrono::milliseconds find_time(rapidjson::Document const &d,
                                    const std::string &key) {
  auto m = d.FindMember(key.c_str());
  if (m != d.MemberEnd() && m->value.IsUint64()) {
    return std::chrono::milliseconds(m->value.GetUint64());
  }
  return std::chrono::milliseconds{0};
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
  rapidjson::Value const *config_stream = nullptr;
};

void CommandHandler::handleNew(std::string const &command) {
  using std::move;
  using std::string;

  nlohmann::json dd;
  try {
    dd = nlohmann::json::parse(command);
  } catch (...) {
    LOG(Sev::Warning, "Can not parse command: {}", command);
    return;
  }

  auto fwt = std::unique_ptr<FileWriterTask>(new FileWriterTask);

  string job_id = dd.at("job_id");
  if (job_id.empty()) {
    LOG(Sev::Warning, "Command not accepted: missing job_id");
    return;
  } else {
    fwt->job_id_init(job_id);
  }

  std::string fname;
  try {
    fname = dd.at("file_attributes").at("file_name");
  } catch (...) {
    fname = "a-dummy-name.h5";
  }

  fwt->set_hdf_filename(Config.hdf_output_prefix, fname);

  rapidjson::Document d;
  d.Parse(command.c_str());

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> stream_hdf_info;
  {
    rapidjson::Value config_file;
    auto &nexus_structure = d.FindMember("nexus_structure")->value;
    auto x = fwt->hdf_init(nexus_structure, config_file, stream_hdf_info);
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
    if (!config_stream_value) {
      LOG(Sev::Notice, "Missing stream specification");
      continue;
    }
    auto attributes = get_object(*stream.config_stream, "attributes");
    auto &config_stream = *config_stream_value.v;
    stream_settings.config_stream = config_stream_value.v;
    LOG(Sev::Info, "Adding stream: {}", json_to_string(config_stream));
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
    auto br = find_broker(d);
    // Must be called before StreamMaster instantiation
    auto start_time = find_time(d, "start_time");
    if (start_time.count()) {
      LOG(Sev::Info, "start time :\t{}", start_time.count());
      Config.StreamerConfiguration.StartTimestamp =
          std::chrono::milliseconds(start_time);
    }

    LOG(Sev::Info, "Write file with id :\t{}", job_id);
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

    auto stop_time = find_time(d, "stop_time");
    if (stop_time.count()) {
      LOG(Sev::Info, "stop time :\t{}", stop_time.count());
      s->setStopTime(std::chrono::milliseconds(stop_time));
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

      hdf_writer_module->parse_config(*stream_settings.config_stream, nullptr);
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

void CommandHandler::handleFileWriterTaskClearAll(nlohmann::json const &d) {
  using namespace rapidjson;
  if (MasterPtr) {
    for (auto &x : MasterPtr->stream_masters) {
      x->stop();
    }
  }
  FileWriterTasks.clear();
}

void CommandHandler::handleExit(nlohmann::json const &d) {
  if (MasterPtr) {
    MasterPtr->stop();
  }
}

void CommandHandler::handleStreamMasterStop(nlohmann::json const &d) {
  using std::string;
  if (!MasterPtr) {
    return;
  }
  string job_id;
  try {
    job_id = d.at("job_id");
  } catch (...) {
    LOG(Sev::Warning, "File write stop message lacks job_id");
    return;
  }
  std::chrono::milliseconds stop_time(0);
  try {
    stop_time = std::chrono::milliseconds(d.at("stop_time"));
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

void CommandHandler::handle(std::string const &command) {
  using std::string;
  using nlohmann::json;
  json d;
  try {
    d = json::parse(command);
  } catch (...) {
    LOG(Sev::Error, "Can not parse json command");
    return;
  }
  uint64_t teamid = 0;
  uint64_t cmd_teamid = 0;
  if (MasterPtr) {
    teamid = MasterPtr->config.teamid;
  }
  try {
    cmd_teamid = d.at("teamid").get<int64_t>();
  } catch (...) {
    // do nothing
  }
  if (cmd_teamid != teamid) {
    LOG(Sev::Info, "INFO command is for teamid {:016x}, we are {:016x}",
        cmd_teamid, teamid);
    return;
  }

  try {
    std::string cmd = d.at("cmd");
    if (cmd == "FileWriter_new") {
      handleNew(command);
      return;
    }
    if (cmd == "FileWriter_exit") {
      handleExit(d);
      return;
    }
    if (cmd == "FileWriter_stop") {
      handleStreamMasterStop(d);
      return;
    }
    if (cmd == "file_writer_tasks_clear_all") {
      try {
        string recv_type = d.at("recv_type");
        if (recv_type == "FileWriter") {
          handleFileWriterTaskClearAll(d);
          return;
        }
      } catch (...) {
      }
    }
  } catch (...) {
    // ignore
  }
  LOG(Sev::Warning, "Could not understand this command: {}", command);
}

void CommandHandler::handle(Msg const &msg) {
  handle({(char *)msg.data(), msg.size()});
}

} // namespace FileWriter
