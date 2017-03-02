#include "Master.h"
#include <chrono>
#include "FileWriterTask.h"
#include "CommandHandler.h"
#include "Source.h"
#include "logger.h"
// #include "helper.h"
// #include "utils.h"
#include "commandproducer.h"

namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;


std::string & CmdMsg_K::str() {
	return _str;
}


Master::Master(MasterConfig & config) :
	config(config),
	command_listener(config.command_listener)
{
}


void Master::handle_command_message(std::unique_ptr<KafkaW::Msg> && msg) {
	CommandHandler command_handler(this);
	command_handler.handle({(char*)msg->data(), (int32_t)msg->size()});
}


void Master::run() {
	command_listener.start();
	if (_cb_on_connected) (*_cb_on_connected)();
	while (do_run) {
		LOG(7, "Master poll");
		auto p = command_listener.poll();
		if (auto msg = p.is_Msg()) {
			LOG(7, "Handle a command");
			this->handle_command_message(std::move(msg));
		}
	}
	LOG(6, "calling stop on all stream_masters");
	for (auto & x : stream_masters) {
	  x->stop();
	}
	LOG(6, "called stop on all stream_masters");
}


void Master::stop() {
	do_run = false;
}


void Master::on_consumer_connected(std::function<void(void)> * cb_on_connected) {
	_cb_on_connected = cb_on_connected;
}


}
}
