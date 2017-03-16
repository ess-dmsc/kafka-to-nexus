#include "CommandHandler.h"
#include "helper.h"
#include "utils.h"

namespace BrightnESS {
  namespace FileWriter {

    // In the future, want to handle many, but not right now.
    static int g_N_HANDLED = 0;

    CommandHandler::CommandHandler(Master * master) : master(master) {
      if (false) {
	using namespace rapidjson;
	auto buf1 = gulp(master->config.dir_assets + "/test/schema-command.json");
	auto doc = make_unique<rapidjson::Document>();
	ParseResult err = doc->Parse(buf1.data(), buf1.size());
	if (err.Code() != ParseErrorCode::kParseErrorNone) {
	  LOG(7, "ERROR can not parse schema_command");
	  throw std::runtime_error("ERROR can not parse schema_command");
	}
	schema_command.reset(new SchemaDocument(*doc));
      }
    }

    void CommandHandler::handle_new(rapidjson::Document & d) {
      //if (g_N_HANDLED > 0) return;
      using namespace rapidjson;
      using std::move;
      if (schema_command) {
	SchemaValidator vali(*schema_command);
	if (!d.Accept(vali)) {
	  StringBuffer sb1, sb2;
	  vali.GetInvalidSchemaPointer().StringifyUriFragment(sb1);
	  vali.GetInvalidDocumentPointer().StringifyUriFragment(sb2);
	  LOG(6, "ERROR command message schema validation:  Invalid schema: {}  keyword: {}",
	      sb1.GetString(),
	      vali.GetInvalidSchemaKeyword()
	      );
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
	auto m1 = d.FindMember("streams");
	if (m1 != d.MemberEnd() && m1->value.IsArray()) {
	  for (auto & st : m1->value.GetArray()) {
	    auto s = Source(st["topic"].GetString(), st["source"].GetString());
	    auto m = st.FindMember("nexus_path");
	    if (m != st.MemberEnd()) {
	      if (m->value.IsString()) {
		s._hdf_path = m->value.GetString();
	      }
	    }
	    fwt->add_source(move(s));
	  }
	}
      }
      uint64_t teamid = 0;
      if (master) {
	teamid = master->config.teamid;
      }
      for (auto & d : fwt->demuxers()) {
	for (auto & s : d.sources()) {
	  s.teamid = teamid;
	}
      }

      for (auto & d : fwt->demuxers()) {
	LOG(7, "{}", d.to_str());
      }

      {
	Value & nexus_structure = d.FindMember("nexus_structure")->value;
	auto x = fwt->hdf_init(nexus_structure);
	if (x) {
	  LOG(7, "ERROR hdf init failed, cancel this write command");
	  return;
	}
      }

      if (master) {
	ESSTimeStamp start_time(0);
	{
	  auto m = d.FindMember("start_time");
#if 0
	  if (m != d.MemberEnd()) {
	    start_time = ESSTimeStamp(m->value.GetInt());
	  }
#endif
	}
	ESSTimeStamp stop_time(0);
	{
	  auto m = d.FindMember("stop_time");
	  if (m != d.MemberEnd()) {
	    stop_time = ESSTimeStamp(m->value.GetInt());
	  }

	}

	std::string br("localhost:9092");
	auto m = d.FindMember("broker");
	if (m != d.MemberEnd()) {
	  br = m->value.GetString();
	}
	auto s = std::unique_ptr< StreamMaster<Streamer, DemuxTopic> >(new StreamMaster<Streamer, DemuxTopic>(br, std::move(fwt)));
	if(start_time.count()) {
	  LOG(3,"start time :\t{}",start_time.count() );
	  s->start_time(start_time);
	}
	if(stop_time.count()) {
	  LOG(3,"stop time :\t{}",stop_time.count() );
	  s->stop_time(stop_time);
	}
	s->start();
	master->stream_masters.push_back(std::move(s));
      }
      else {
	file_writer_tasks.emplace_back(std::move(fwt));
      }
      g_N_HANDLED += 1;
    }

    void CommandHandler::handle_exit(rapidjson::Document & d) {
      if (master) master->stop();
    }

    void CommandHandler::handle(Msg const & msg) {
      using namespace rapidjson;
      auto doc = make_unique<Document>();
      ParseResult err = doc->Parse((char*)msg.data, msg.size);
      if (doc->HasParseError()) {
	LOG(2, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
	return;
      }
      auto & d = * doc;

      uint64_t teamid = 0;
      uint64_t cmd_teamid = 0;
      if (master) {
	teamid = master->config.teamid;
      }
      if (d.HasMember("teamid")) {
	auto & m = d["teamid"];
	if (m.IsInt()) {
	  cmd_teamid = d["teamid"].GetUint64();
	}
      }
      if (cmd_teamid != teamid) {
	LOG(1, "INFO command is for teamid {:016x}, we are {:016x}", cmd_teamid, teamid);
	return;
      }

      if (d.HasMember("cmd")) {
	if (d["cmd"].IsString()) {
	  std::string cmd(d["cmd"].GetString());
	  if (cmd == "FileWriter_new") {
	    handle_new(d);
	    return;
	  }
	  if (cmd == "FileWriter_exit") {
	    handle_exit(d);
	    return;
	  }
	}
      }
      {
	auto n1 = std::min(msg.size, (int32_t)1024);
	LOG(3, "ERROR could not figure out this command: {:.{}}", msg.data, n1);
      }
    }

  }
}
