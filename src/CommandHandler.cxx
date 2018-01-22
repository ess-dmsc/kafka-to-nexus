#include "CommandHandler.h"
#include "FileWriterTask.h"
#include "HDFWriterModule.h"
#include "MPIChild.h"
#include "helper.h"
#include "utils.h"
#include <future>
#include <h5.h>

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
      LOG(Sev::Error, "ERROR can not parse schema_command");
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
      LOG(Sev::Error, "ERROR hdf init failed, cancel this write command");
      return;
    }
  }

#if USE_PARALLEL_WRITER
  auto &jm = config.jm;
  jm->use_this();
  fwt->cq = CollectiveQueue::ptr(new CollectiveQueue(jm));
  jm->use_default();
#else
  fwt->cq = std::unique_ptr<CollectiveQueue>();
#endif

  LOG(Sev::Info, "Command contains {} streams", stream_hdf_info.size());
  for (auto &stream : stream_hdf_info) {
    auto config_stream_value = get_object(*stream.config_stream, "stream");
    if (!config_stream_value) {
      LOG(Sev::Notice, "Missing stream specification");
      continue;
    }
    auto attributes = get_object(*stream.config_stream, "attributes");
    auto &config_stream = *config_stream_value.v;
    LOG(Sev::Info, "Adding stream: {}", json_to_string(config_stream));
    auto topic = get_string(&config_stream, "topic");
    if (!topic) {
      LOG(Sev::Notice, "Missing topic on stream specification");
      continue;
    }
    auto source = get_string(&config_stream, "source");
    if (!source) {
      LOG(Sev::Notice, "Missing source on stream specification");
      continue;
    }
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
    bool run_parallel = false;
    auto run_parallel_cfg = get_bool(&config_stream, "run_parallel");
    if (run_parallel_cfg) {
      run_parallel = run_parallel_cfg.v;
      if (run_parallel) {
        LOG(5, "Run parallel {}", source.v);
      }
    }

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

    hdf_writer_module->parse_config(config_stream, nullptr);
    hdf_writer_module->init_hdf(fwt->hdf_file.h5file, stream.hdf_parent_name,
                                fwt->cq.get(), attributes.v);
    LOG(8, "close");
    hdf_writer_module->close();
    LOG(8, "reset");
    hdf_writer_module.reset();
  }

  fwt->hdf_file.close();
  fwt->hdf_file.cq = fwt->cq.get();
  fwt->hdf_file.reopen(fname, rapidjson::Value());

#if USE_PARALLEL_WRITER
  bool use_parallel_writer = true;
  fwt->hdf_store.mpi_rank = 0;
  fwt->hdf_store.cqid = fwt->hdf_file.cq->open(fwt->hdf_store);
  fwt->hdf_store.h5file = fwt->hdf_file.h5file;
#else
  bool use_parallel_writer = false;
#endif

  std::vector<MPIChild::ptr> to_spawn;

  for (auto &stream : stream_hdf_info) {
    // TODO
    // Refactor with the above loop..
    auto config_stream_value = get_object(*stream.config_stream, "stream");
    if (!config_stream_value) {
      LOG(5, "Missing stream specification");
      continue;
    }
    auto &config_stream = *config_stream_value.v;
    // LOG(7, "Adding stream: {}", json_to_string(config_stream));
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
    bool run_parallel = false;
    auto run_parallel_cfg = get_bool(&config_stream, "run_parallel");
    if (run_parallel_cfg) {
      run_parallel = run_parallel_cfg.v;
      if (run_parallel) {
        LOG(5, "Run parallel {}", source.v);
      }
    }

    // for each stream:
    //   either re-open in this main process
    //     Re-create HDFWriterModule
    //     Re-parse the stream config
    //     Re-open HDF items
    //     Create a Source which feeds directly to that module
    LOG(8, "topic: {}  use_parallel_writer: {}  run_parallel: {}", topic.v,
        use_parallel_writer, run_parallel);
    if (!use_parallel_writer || !run_parallel) {
      LOG(8, "add Source as non-parallel: {}", topic.v);
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

      hdf_writer_module->parse_config(config_stream, nullptr);
#if USE_PARALLEL_WRITER
      auto err = hdf_writer_module->reopen(fwt->hdf_file.h5file,
                                           stream.hdf_parent_name,
                                           fwt->cq.get(), &fwt->hdf_store);
#else
      auto err = hdf_writer_module->reopen(
          fwt->hdf_file.h5file, stream.hdf_parent_name, nullptr, nullptr);
#endif
      if (err.is_ERR()) {
        LOG(3, "can not reopen HDF file for stream {}", stream.hdf_parent_name);
        exit(1);
      }

#if USE_PARALLEL_WRITER
      {
        int rank_merged = 0;
        hdf_writer_module->enable_cq(fwt->cq.get(), &fwt->hdf_store,
                                     rank_merged);
      }
#endif

      auto s = Source(source.v, move(hdf_writer_module), config.jm, config.shm,
                      fwt->cq.get());
      s._topic = string(topic);
      s.do_process_message = config.source_do_process_message;
      fwt->add_source(move(s));
    }

    else {
      //   or re-open in one or more separate mpi workers Send command to
      //   create HDFWriterModule, all the json config as text, let it re-open
      //   hdf items Create a Source which puts messages on a queue
      LOG(8, "add Source as parallel: {}", topic.v);
      auto s = Source(source.v, {}, config.jm, config.shm, fwt->cq.get());
      s.is_parallel = true;
      s._topic = string(topic);
      s.do_process_message = config.source_do_process_message;
      {
        Document c1;
        c1.CopyFrom(config.config_file, c1.GetAllocator());
        Document c2;
        c2.CopyFrom(d, c2.GetAllocator());
        Document c3;
        auto &c3a = c3.GetAllocator();
        c3.CopyFrom(config_stream, c3a);
        c3.AddMember("hdf_parent_name",
                     Value(stream.hdf_parent_name.c_str(), c3a), c3a);
        s.mpi_start(move(c1), move(c2), move(c3), to_spawn);
      }
      fwt->add_source(move(s));
    }
  }

#if USE_PARALLEL_WRITER
  fwt->mpi_start(std::move(to_spawn));
#endif

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
      LOG(Sev::Info, "start time :\t{}", start_time.count());
      s->start_time(start_time);
    }
    auto stop_time = find_time(d, "stop_time");
    if (stop_time.count()) {
      LOG(Sev::Info, "stop time :\t{}", stop_time.count());
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
        LOG(Sev::Info, "gracefully stop file with id : {}", job_id);
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
