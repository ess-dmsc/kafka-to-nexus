#include "Master.h"
#include <chrono>
#include "FileWriterTask.h"
#include "NexusWriter.h"
#include "Source.h"
#include "logger.h"
#include "helper.h"
#include <rapidjson/document.h>
#include <rapidjson/schema.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>

#include "f140-general_generated.h"
#include "f141-ntarraydouble_generated.h"

namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;


std::string & CmdMsg_K::str() {
	return _str;
}


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

void handle(std::unique_ptr<CmdMsg> msg) {
	using namespace rapidjson;
	auto doc = make_unique<Document>();
	ParseResult err = doc->Parse(msg->str().data(), msg->str().size());
	if (doc->HasParseError()) {
		LOG(6, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
		throw std::runtime_error("");
	}
	auto & d = * doc;
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
	fwt->set_hdf_filename("a-dummy-name.h5");

	for (auto & st : d["streams"].GetArray()) {
		fwt->add_source(Source(st["topic"].GetString(), st["source"].GetString()));
	}

	for (auto & d : fwt->demuxers()) {
		LOG(1, "{}", d.to_str());
	}

	fwt->hdf_init();

	{
		// TODO
		// Move testing code into async test
		for (int i1 = 0; i1 < 3; ++i1) {
			flatbuffers::FlatBufferBuilder builder(1024);
			auto srcn = builder.CreateString(fwt->demuxers().at(0).sources().at(0).source());
			std::vector<double> data;
			data.resize(5);
			for (size_t i2 = 0; i2 < data.size(); ++i2) {
				data[i2] = 10000 + 100 * i1 + i2;
			}
			auto v = builder.CreateVector(data);
			BrightnESS::ForwardEpicsToKafka::FlatBufs::f141_ntarraydouble::PVBuilder b1(builder);
			b1.add_ts(102030);
			b1.add_src(srcn);
			b1.add_v(v);
			auto pv = b1.Finish();
			builder.Finish(pv);
			std::vector<char> msg;
			msg.push_back(0x41);
			msg.push_back(0xf1);
			std::copy(builder.GetBufferPointer(), builder.GetBufferPointer() + builder.GetSize(), std::back_inserter(msg));
			fwt->demuxers().at(0).process_message(msg.data(), msg.size());
		}
		fwt->file_flush();
	}
}

private:
std::unique_ptr<rapidjson::SchemaDocument> schema_command;
Master * master;
};



Master::Master(MasterConfig config) :
	config(config),
	command_listener(config.command_listener)
{
	if (config.test_mockup_command_listener) {
		command_listener.is_mockup = true;
	}
}


void Master::handle_command_message(std::unique_ptr<CmdMsg> && msg) {
	CommandHandler command_handler(this);
	command_handler.handle(std::move(msg));
}


void Master::run() {
	using CLK = std::chrono::steady_clock;
	auto start = CLK::now();
	command_listener.start();
	if (_cb_on_connected) (*_cb_on_connected)();
	while (do_run) {
		LOG(0, "Master poll");
		auto p = command_listener.poll();
		if (auto msg = p.is_CmdMsg()) {
			this->handle_command_message(std::move(msg));
			// TODO
			// Allow to set a callback so that tests can exit the poll loop
			do_run = false;
		}
		auto now = CLK::now();
		if (now - start > std::chrono::milliseconds(8000)) {
			break;
		}
	}
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

TEST(setup_with_kafka, setup_01) {
	using namespace BrightnESS::FileWriter;
	MasterConfig conf_m;
	TestCommandProducer tcp;
	auto of = tcp.produce_simple_01(conf_m.command_listener);
	conf_m.command_listener.start_at_command_offset = of;
	Master m(conf_m);
	ASSERT_NO_THROW( m.run() );
}

#endif
