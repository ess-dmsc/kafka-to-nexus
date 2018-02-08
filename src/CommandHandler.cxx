#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "helper.h"
#include <future>

using std::array;
using std::vector;

namespace FileWriter {

std::string find_filename(rapidjson::Document const &d) {
  auto m1 = d.FindMember("file_attributes");
  if (m1 != d.MemberEnd() && m1->value.IsObject()) {
    auto m2 = m1->value.FindMember("file_name");
    if (m2 != m1->value.MemberEnd() && m2->value.IsString()) {
      return m2->value.GetString();
    }
  }
  return std::string{"a-dummy-name.h5"};
}

std::string find_job_id(rapidjson::Document const &d) {
  auto m = d.FindMember("job_id");
  if (m != d.MemberEnd() && m->value.IsString()) {
    return m->value.GetString();
  }
  return std::string{""};
}

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

CommandHandler::CommandHandler(MainOpt &config, Master *master)
    : config(config), master(master) {
  // Will take care of this in upcoming PR.
  if (false) {
    using namespace rapidjson;
    auto buf1 = gulp("/test/schema-command.json");
    auto doc = make_unique<rapidjson::Document>();
    ParseResult err = doc->Parse(buf1.data(), buf1.size());
    if (err.Code() != ParseErrorCode::kParseErrorNone) {
      LOG(Sev::Error, "ERROR can not parse schema_command");
      throw std::runtime_error("ERROR can not parse schema_command");
    }
    schema_command.reset(new SchemaDocument(*doc));
  }
}

// POD

struct StreamSettings {
  StreamHDFInfo stream_hdf_info;
  std::string topic;
  std::string module;
  std::string source;
  bool run_parallel = false;
  rapidjson::Value const *config_stream = nullptr;
};

void CommandHandler::handle_new(rapidjson::Document const &d) {
  // if (g_N_HANDLED > 0) return;
  using namespace rapidjson;
  using std::move;
  using std::string;
  if (schema_command) {
    SchemaValidator vali(*schema_command);
    if (!d.Accept(vali)) {
      StringBuffer sb1, sb2;
      vali.GetInvalidSchemaPointer().StringifyUriFragment(sb1);
      vali.GetInvalidDocumentPointer().StringifyUriFragment(sb2);
      LOG(Sev::Warning,
          "ERROR command message schema validation:  Invalid schema: {}  "
          "keyword: {}",
          sb1.GetString(), vali.GetInvalidSchemaKeyword());
      return;
    }
  }

  auto fwt = std::unique_ptr<FileWriterTask>(new FileWriterTask);

  auto job_id = find_job_id(d);
  if (!job_id.empty()) {
    fwt->job_id_init(job_id);
  } else {
    LOG(Sev::Warning, "Command not accepted: missing job_id");
    return;
  }

  auto fname = find_filename(d);
  fwt->set_hdf_filename(config.hdf_output_prefix, fname);

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

  add_stream_source_to_writer_module(stream_settings_list, fwt);

  if (master) {
    auto br = find_broker(d);
    // Must be called before StreamMaster instantiation
    auto start_time = find_time(d, "start_time");
    if (start_time.count()) {
      LOG(Sev::Info, "start time :\t{}", start_time.count());
      config.StreamerConfiguration.StartTimestamp =
          std::chrono::milliseconds(start_time);
    }

    LOG(Sev::Info, "Write file with id :\t{}", job_id);
    auto s = std::unique_ptr<StreamMaster<Streamer>>(new StreamMaster<Streamer>(
        br, std::move(fwt), config.StreamerConfiguration));
    if (master->status_producer) {
      s->report(master->status_producer,
                std::chrono::milliseconds{config.status_master_interval});
    }
    if (config.topic_write_duration.count()) {
      s->TopicWriteDuration = config.topic_write_duration;
    }
    s->start();

    auto stop_time = find_time(d, "stop_time");
    if (stop_time.count()) {
      LOG(Sev::Info, "stop time :\t{}", stop_time.count());
      s->setStopTime(std::chrono::milliseconds(stop_time));
    }

    master->stream_masters.push_back(std::move(s));
  } else {
    file_writer_tasks.emplace_back(std::move(fwt));
  }
  g_N_HANDLED += 1;
}

void CommandHandler::add_stream_source_to_writer_module(
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
      s.do_process_message = config.source_do_process_message;
      fwt->add_source(std::move(s));
    }
  }
}

void CommandHandler::handle_file_writer_task_clear_all(
    rapidjson::Document const &d) {
  using namespace rapidjson;
  if (master) {
    for (auto &x : master->stream_masters) {
      x->stop();
    }
  }
  file_writer_tasks.clear();
}

void CommandHandler::handle_exit(rapidjson::Document const &d) {
  if (master)
    master->stop();
}

void CommandHandler::handle_stream_master_stop(rapidjson::Document const &d) {
  if (master) {
    auto s = get_string(&d, "job_id");
    auto job_id = std::string(s);
    std::chrono::milliseconds stop_time(0);
    auto m = d.FindMember("stop_time");
    if (m != d.MemberEnd()) {
      stop_time = std::chrono::milliseconds(m->value.GetUint64());
    }
    int counter{0};
    for (auto &x : master->stream_masters) {
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
}

void CommandHandler::handle(rapidjson::Document const &d) {
  using std::string;
  using namespace rapidjson;
  uint64_t teamid = 0;
  uint64_t cmd_teamid = 0;
  if (master) {
    teamid = master->config.teamid;
  }
  if (auto i = get_int(&d, "teamid")) {
    cmd_teamid = int64_t(i);
  }
  if (cmd_teamid != teamid) {
    LOG(Sev::Info, "INFO command is for teamid {:016x}, we are {:016x}",
        cmd_teamid, teamid);
    return;
  }

  // The ways to give commands will be unified in upcoming PR.
  if (auto s = get_string(&d, "cmd")) {
    auto cmd = string(s);
    if (cmd == "FileWriter_new") {
      handle_new(d);
      return;
    }
    if (cmd == "FileWriter_exit") {
      handle_exit(d);
      return;
    }
    if (cmd == "FileWriter_stop") {
      handle_stream_master_stop(d);
      return;
    }
  }

  if (auto s = get_string(&d, "recv_type")) {
    auto recv_type = string(s);
    if (recv_type == "FileWriter") {
      if (auto s = get_string(&d, "cmd")) {
        auto cmd = string(s);
        if (cmd == "file_writer_tasks_clear_all") {
          handle_file_writer_task_clear_all(d);
          return;
        }
      }
    }
  }

  StringBuffer buffer;
  PrettyWriter<StringBuffer> writer(buffer);
  d.Accept(writer);
  LOG(Sev::Warning, "ERROR could not figure out this command: {}",
      buffer.GetString());
}

void CommandHandler::handle(Msg const &msg) {
  using std::string;
  using namespace rapidjson;
  auto doc = make_unique<Document>();
  ParseResult err = doc->Parse((char *)msg.data(), msg.size());
  if (doc->HasParseError()) {
    LOG(Sev::Warning, "ERROR json parse: {} {}", err.Code(),
        GetParseError_En(err.Code()));
    return;
  }
  handle(*doc);
}

} // namespace FileWriter
