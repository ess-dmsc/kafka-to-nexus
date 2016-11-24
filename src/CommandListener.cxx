#include "CommandListener.h"
#include <string>
#include "logger.h"
#include "kafka_util.h"

namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;

CommandListener::CommandListener(CommandListenerConfig config) : config(config) { }

void CommandListener::start() {
	string errstr;
	gconf = decltype(gconf)(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	gconf->set("metadata.broker.list", config.address, errstr);
	tconf = decltype(tconf)(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
	kcons = decltype(kcons)(RdKafka::Consumer::create(gconf.get(), errstr));
	if (not kcons) {
		LOG(3, "ERROR can not create the consumer {}", errstr);
		throw BrokerFailure(errstr);
	}

	topic = decltype(topic)(RdKafka::Topic::create(kcons.get(), config.topic, tconf.get(), errstr));
	if (not topic) {
		LOG(3, "ERROR can not create the topic: {}", errstr);
		throw BrokerFailure(errstr);
	}

	auto start_offset = RdKafka::Topic::OFFSET_BEGINNING;
	auto err = kcons->start(topic.get(), partition, start_offset);
	if (err != RdKafka::ERR_NO_ERROR) {
		errstr = RdKafka::err2str(err);
		LOG(3, "ERROR can not start Consumer {}", errstr);
		throw BrokerFailure(errstr);
	}
}


class ConsumeCallback : public RdKafka::ConsumeCb {
public:
void consume_cb(RdKafka::Message & msg, void * opaque) {
	switch (msg.err()) {
	case RdKafka::ERR__TIMED_OUT:
		break;

	case RdKafka::ERR_NO_ERROR:
		//msg.len();
		//msg.payload();
		//msg.offset();
		//msg.key();  can be nullptr
		break;

	case RdKafka::ERR__PARTITION_EOF:
		// Last message
		break;

	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		//msg.errstr()
		break;

	default:
		//msg.errstr()
		break;
	}
}
};


void CommandListener::poll(FileWriterCommandHandler & command_handler) {
	ConsumeCallback ccb;
	auto top = topic.get();
	void * opaque = nullptr;
	//std::this_thread::sleep_for(std::chrono::milliseconds(10));
	auto nmsg = kcons->consume_callback(top, partition, 10, &ccb, opaque);
	LOG(3, "Processed commands: {}", nmsg);
	auto npoll = kcons->poll(0);
	LOG(3, "Poll processed: {}", npoll);
}



}
}
