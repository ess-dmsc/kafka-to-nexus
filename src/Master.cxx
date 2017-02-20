#include "Master.h"
#include <chrono>
#include "FileWriterTask.h"
#include "NexusWriter.h"
#include "Source.h"
#include "logger.h"
#include "helper.h"
#include "commandproducer.h"
#include <rapidjson/document.h>
#include <rapidjson/schema.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>

namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;


std::string & CmdMsg_K::str() {
	return _str;
}


// In the future, want to handle many, but not right now.
int g_N_HANDLED = 0;

/// Stub, will perform the JSON parsing and then take appropriate action.
class CommandHandler : public FileWriterCommandHandler {
public:

CommandHandler(Master * master) : master(master) {
	using namespace rapidjson;
	auto buf1 = gulp("test/schema-command.json");
	auto doc = make_unique<rapidjson::Document>();
	ParseResult err = doc->Parse(buf1.data(), buf1.size());
	if (err.Code() != ParseErrorCode::kParseErrorNone) {
		LOG(7, "ERROR can not parse schema_command");
		throw std::runtime_error("ERROR can not parse schema_command");
	}
	schema_command.reset(new SchemaDocument(*doc));
}

void handle_new(rapidjson::Document & d) {
	if (g_N_HANDLED > 0) return;

	using namespace rapidjson;
	SchemaValidator vali(*schema_command);
	if (!d.Accept(vali)) {
		StringBuffer sb1, sb2;
		vali.GetInvalidSchemaPointer().StringifyUriFragment(sb1);
		vali.GetInvalidDocumentPointer().StringifyUriFragment(sb2);
		LOG(6, "ERROR command message schema validation:  Invalid schema: {}  keyword: {}",
			sb1.GetString(),
			vali.GetInvalidSchemaKeyword()
		);
		throw std::runtime_error("ERROR command message schema validation");
	}

	LOG(1, "cmd: {}", d["cmd"].GetString());
	auto fwt = std::unique_ptr<FileWriterTask>(new FileWriterTask);
	std::string fname = "a-dummy-name.h5";
	if (d.HasMember("filename")) {
		if (d["filename"].IsString()) {
			fname = d["filename"].GetString();
		}
	}
	fwt->set_hdf_filename(fname);

	for (auto & st : d["streams"].GetArray()) {
		fwt->add_source(Source(st["topic"].GetString(), st["source"].GetString()));
	}
	for (auto & d : fwt->demuxers()) {
		for (auto & s : d.sources()) {
			s.teamid = master->config.teamid;
		}
	}

	for (auto & d : fwt->demuxers()) {
		LOG(1, "{}", d.to_str());
	}

	fwt->hdf_init();

	std::string br(d["broker"].GetString());
	auto s = std::unique_ptr< StreamMaster<Streamer, DemuxTopic> >(new StreamMaster<Streamer, DemuxTopic>(br, std::move(fwt)));
	s->start();
	master->stream_masters.push_back(std::move(s));
	g_N_HANDLED += 1;
}

void handle_exit(rapidjson::Document & d) {
	master->stop();
}

void handle(std::unique_ptr<KafkaW::Msg> msg) {
	using namespace rapidjson;
	auto doc = make_unique<Document>();
	ParseResult err = doc->Parse((char*)msg->data(), msg->size());
	if (doc->HasParseError()) {
		LOG(6, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
		throw std::runtime_error("");
	}
	auto & d = * doc;

	auto teamid = 0;
	if (d.HasMember("teamid")) {
		auto & m = d["teamid"];
		if (m.IsInt()) {
			teamid = d["teamid"].GetInt();
		}
	}
	if (teamid != master->config.teamid) {
		LOG(2, "WARNING command is for teamid {}, we are {}", teamid, master->config.teamid);
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
	LOG(3, "ERROR could not figure out this command: {:.{}}", msg->data(), msg->size());

}

private:
std::unique_ptr<rapidjson::SchemaDocument> schema_command;
Master * master;
};



Master::Master(MasterConfig config) :
	config(config),
	command_listener(config.command_listener)
{
}


void Master::handle_command_message(std::unique_ptr<KafkaW::Msg> && msg) {
	CommandHandler command_handler(this);
	command_handler.handle(std::move(msg));
}


void Master::run() {
	command_listener.start();
	if (_cb_on_connected) (*_cb_on_connected)();
	while (do_run) {
		LOG(0, "Master poll");
		auto p = command_listener.poll();
		if (auto msg = p.is_Msg()) {
			LOG(0, "Handle a command");
			this->handle_command_message(std::move(msg));
		}
	}
	LOG(1, "calling stop on all stream_masters");
	for (auto & x : stream_masters) {
		x->stop(-2);
	}
	LOG(1, "called stop on all stream_masters");
}


void Master::stop() {
	do_run = false;
}


void Master::on_consumer_connected(std::function<void(void)> * cb_on_connected) {
	_cb_on_connected = cb_on_connected;
}


}
}




#if HAVE_GTEST
#include <gtest/gtest.h>

TEST(config, read_simple) {
	return;
	LOG(3, "Test a simple configuration");
	using namespace BrightnESS::FileWriter;
	// TODO
	// * Input a predefined configuration message to setup a simple stream writing
	// * Connect outputs to test buffers
	// * Input a predefined message (or more) and test if it arrives at the correct ends
	MasterConfig conf_m;
	conf_m.test_mockup_command_listener = true;
	Master m(conf_m);
	ASSERT_NO_THROW( m.run() );
}

#endif
