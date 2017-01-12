#include "Master.h"
#include <chrono>
#include "NexusWriter.h"
#include "logger.h"
#include "helper.h"
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
		LOG(3, "ERROR json parse: {} {}", err.Code(), GetParseError_En(err.Code()));
		throw std::runtime_error("");
	}
	auto & d = * doc;
	SchemaValidator vali(*schema_command);
	if (not d.Accept(vali)) {
		StringBuffer sb1, sb2;
		vali.GetInvalidSchemaPointer().StringifyUriFragment(sb1);
		vali.GetInvalidDocumentPointer().StringifyUriFragment(sb2);
		LOG(3, "ERROR command message schema validation:  Invalid schema: {}  keyword: {}",
			sb1.GetString(),
			vali.GetInvalidSchemaKeyword()
		);
		throw std::runtime_error("ERROR command message schema validation");
	}
	LOG(3, "cmd: {}", d["cmd"].GetString());

	try {
		master->nexus_writers.emplace_back(new NexusWriter(d));
	}
	catch (...) {
		LOG(3, "TODO see what we can handle here...");
		throw;
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


void Master::run() {
	using CLK = std::chrono::steady_clock;
	auto start = CLK::now();
	command_listener.start();
	// Handler is meant to live only until the command is handled
	CommandHandler command_handler(this);
	if (_cb_on_connected) (*_cb_on_connected)();
	while (do_run) {
		LOG(0, "Master poll");
		auto p = command_listener.poll(command_handler);
		if (auto msg = p.is_CmdMsg()) {
			LOG(9, "Master got: {}", msg->str());
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
	std::function<int64_t()> cb = [conf_m] {
		//std::thread t1 ( [conf_m] {
			//std::this_thread::sleep_for(std::chrono::milliseconds(10));
			TestCommandProducer tcp;
			auto res = tcp.produce_simple_01(conf_m.command_listener);
			return res;
		//});
		//t1.join();
	};
	//conf_m.command_listener.on_rebalance_assign = &cb;
	auto of = cb();
	conf_m.command_listener.start_at_command_offset = of;

	if (1) {
		Master m(conf_m);
		ASSERT_NO_THROW( m.run() );
	}
}

#endif
