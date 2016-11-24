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
	auto gconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	gconf->set("metadata.broker.list", config.address, errstr);
	auto tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	auto kcons = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(gconf.get(), errstr));
	if (not kcons) {
		LOG(3, "ERROR can not create the consumer {}", errstr);
		throw BrokerFailure(errstr);
	}

	auto topic = RdKafka::Topic::create(kcons.get(), config.topic, tconf, errstr);
	if (not topic) {
		LOG(3, "ERROR can not create the topic: {}", errstr);
		throw BrokerFailure(errstr);
	}

	auto partition = RdKafka::Topic::PARTITION_UA;
	auto start_offset = RdKafka::Topic::OFFSET_BEGINNING;
	auto err = kcons->start(topic, partition, start_offset);
	if (err != RdKafka::ERR_NO_ERROR) {
		errstr = RdKafka::err2str(err);
		LOG(3, "ERROR can not start Consumer {}", errstr);
		throw BrokerFailure(errstr);
	}

	// Run the configuration listener in its own thread to avoid a hickup to propagate to the writers
	thr_consumer = std::thread(&CommandListener::run, this);
}


void CommandListener::run() {
	while (true) {
		// TODO this is a dummy so far
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
	}
}


}
}
