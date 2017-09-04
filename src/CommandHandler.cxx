#include "CommandHandler.h"
#include "HDFWriterModule.h"
#include "helper.h"
#include "utils.h"
#include <h5.h>

#include <future>

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

ESSTimeStamp find_time(rapidjson::Document const &d, const std::string &key) {
  auto m = d.FindMember(key.c_str());
  if (m != d.MemberEnd() && m->value.IsUint64()) {
    return ESSTimeStamp(m->value.GetUint64());
  }
  return ESSTimeStamp{0};
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
      LOG(7, "ERROR can not parse schema_command");
      throw std::runtime_error("ERROR can not parse schema_command");
    }
    schema_command.reset(new SchemaDocument(*doc));
  }
}

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
      LOG(6, "ERROR command message schema validation:  Invalid schema: {}  "
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
    LOG(2, "Command not accepted: missing job_id");
    return;
  }

  auto fname = find_filename(d);
  fwt->set_hdf_filename(fname);

  // When FileWriterTask::hdf_init() returns, `stream_hdf_info` will contain
  // the list of streams which have been found in the `nexus_structure`.
  std::vector<StreamHDFInfo> stream_hdf_info;
  std::vector<hid_t> groups;
  {
    rapidjson::Value config_file;
    auto &nexus_structure = d.FindMember("nexus_structure")->value;
    auto x =
        fwt->hdf_init(nexus_structure, config_file, stream_hdf_info, groups);
    if (x) {
      LOG(7, "ERROR hdf init failed, cancel this write command");
      return;
    }
  }

  LOG(6, "Command contains {} streams", stream_hdf_info.size());
  for (auto &stream : stream_hdf_info) {
    auto config_stream_value = get_object(*stream.config_stream, "stream");
    if (!config_stream_value) {
      LOG(5, "Missing stream specification");
      continue;
    }
    auto &config_stream = *config_stream_value.v;
    LOG(7, "Adding stream: {}", json_to_string(config_stream));
    auto topic = get_string(&config_stream, "topic");
    if (!topic) {
      LOG(5, "Missing topic on stream specification");
      continue;
    }
    auto source = get_string(&config_stream, "source");
    if (!source) {
      LOG(5, "Missing source on stream specification");
      continue;
    }
    auto module = get_string(&config_stream, "module");
    if (!module) {
      LOG(5, "Missing module on stream specification");
      continue;
    }

    auto module_factory = HDFWriterModuleRegistry::find(module.v);
    if (!module_factory) {
      LOG(5, "Module '{}' is not available", module.v);
      continue;
    }

    auto hdf_writer_module = module_factory();
    if (!hdf_writer_module) {
      LOG(5, "Can not create a HDFWriterModule for '{}'", module.v);
      continue;
    }

    hdf_writer_module->init_hdf(fwt->hdf_file.h5file, stream.name,
                                config_stream, nullptr);

    auto s = Source(source.v, move(hdf_writer_module));
    // Can this be done in a better way?
    s._topic = string(topic);
    s.do_process_message = config.source_do_process_message;
    fwt->add_source(move(s));
  }

  for (auto &id : groups) {
    herr_t err = 0;
    err = H5Gclose(id);
    if (err < 0) {
      LOG(3, "failed H5Gclose");
    }
  }

  if (master) {
    auto br = find_broker(d);
    auto config_kafka = config.kafka;
    std::vector<std::pair<string, string>> config_kafka_vec;
    for (auto &x : config_kafka) {
      config_kafka_vec.emplace_back(x.first, x.second);
    }

    auto s = std::unique_ptr<StreamMaster<Streamer>>(
        new StreamMaster<Streamer>(br, std::move(fwt), config_kafka_vec));
    if (master->status_producer) {
      s->report(master->status_producer, config.status_master_interval);
    }
    auto start_time = find_time(d, "start_time");
    if (start_time.count()) {
      LOG(6, "start time :\t{}", start_time.count());
      s->start_time(start_time);
    }
    auto stop_time = find_time(d, "stop_time");
    if (stop_time.count()) {
      LOG(6, "stop time :\t{}", stop_time.count());
      s->stop_time(stop_time);
    }
    s->start();
    master->stream_masters.push_back(std::move(s));
  } else {
    file_writer_tasks.emplace_back(std::move(fwt));
  }
  g_N_HANDLED += 1;
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
    ESSTimeStamp stop_time{0};
    {
      auto m = d.FindMember("stop_time");
      if (m != d.MemberEnd()) {
        stop_time = ESSTimeStamp(m->value.GetUint64());
      }
    }
    int counter{0};
    for (auto &x : master->stream_masters) {
      if (x->job_id() == job_id) {
        if (stop_time.count()) {
          x->stop_time(stop_time);
        } else {
          x->stop();
        }
        LOG(5, "gracefully stop file with id : {}", job_id);
        ++counter;
      }
    }

    if (counter == 0) {
      LOG(3, "no file with id : {}", job_id);
    } else if (counter > 1) {
      LOG(3, "error: multiple files with id : {}", job_id);
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
    LOG(1, "INFO command is for teamid {:016x}, we are {:016x}", cmd_teamid,
        teamid);
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
  LOG(3, "ERROR could not figure out this command: {}", buffer.GetString());
}

void CommandHandler::handle(Msg const &msg) {
  using std::string;
  using namespace rapidjson;
  auto doc = make_unique<Document>();
  ParseResult err = doc->Parse((char *)msg.data(), msg.size());
  if (doc->HasParseError()) {
    LOG(2, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
    return;
  }
  handle(*doc);
}

} // namespace FileWriter
