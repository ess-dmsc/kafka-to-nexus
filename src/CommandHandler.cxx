#include "CommandHandler.h"
#include "helper.h"
#include "utils.h"

namespace FileWriter {

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
  std::string fname = "a-dummy-name.h5";
  {
    auto m1 = d.FindMember("file_attributes");
    if (m1 != d.MemberEnd() && m1->value.IsObject()) {
      auto m2 = m1->value.FindMember("file_name");
      if (m2 != m1->value.MemberEnd() && m2->value.IsString()) {
        fname = m2->value.GetString();
      }
    }
  }
  fwt->set_hdf_filename(fname);

  {
    auto &nexus_structure = d.FindMember("nexus_structure")->value;
    auto x = fwt->hdf_init(nexus_structure);
    if (x) {
      LOG(7, "ERROR hdf init failed, cancel this write command");
      return;
    }
  }

  {
    auto m1 = d.FindMember("streams");
    if (m1 != d.MemberEnd() && m1->value.IsArray()) {
      for (auto &st : m1->value.GetArray()) {
        auto s = Source(st["topic"].GetString(), st["source"].GetString());
        auto m = st.FindMember("nexus_path");
        if (m != st.MemberEnd()) {
          if (m->value.IsString()) {
            s._hdf_path = m->value.GetString();
          }
        }
        s.config_file(&config.config_file);
        rapidjson::Document j1;
        j1.CopyFrom(st, j1.GetAllocator());
        s.config_stream(std::move(j1));
        fwt->add_source(move(s));
      }
    }
  }

  if (master) {
    ESSTimeStamp start_time(0);
    {
      auto m = d.FindMember("start_time");
      if (m != d.MemberEnd()) {
        start_time = ESSTimeStamp(m->value.GetUint64());
      }
    }
    ESSTimeStamp stop_time(0);
    {
      auto m = d.FindMember("stop_time");
      if (m != d.MemberEnd()) {
        stop_time = ESSTimeStamp(m->value.GetUint64());
      }
    }

    std::string br("localhost:9092");
    auto m = d.FindMember("broker");
    if (m != d.MemberEnd()) {
      auto s = std::string(m->value.GetString());
      if (s.substr(0, 2) == "//") {
        uri::URI u(s);
        br = u.host_port;
      } else {
        // legacy semantics
        br = s;
      }
    }

    auto config_kafka = config.kafka;
    std::vector<std::pair<string, string>> config_kafka_vec;
    for (auto &x : config_kafka) {
      config_kafka_vec.emplace_back(x.first, x.second);
    }

    // TODO
    // Actually pass master->status_producer to StreamMaster here

    auto s = std::unique_ptr<StreamMaster<Streamer, DemuxTopic>>(
        new StreamMaster<Streamer, DemuxTopic>(br, std::move(fwt),
                                               config_kafka_vec));
    if (start_time.count()) {
      LOG(3, "start time :\t{}", start_time.count());
      s->start_time(start_time);
    }
    if (stop_time.count()) {
      LOG(3, "stop time :\t{}", stop_time.count());
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
  ParseResult err = doc->Parse((char *)msg.data, msg.size);
  if (doc->HasParseError()) {
    LOG(2, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
    return;
  }
  handle(*doc);
}

} // namespace FileWriter
