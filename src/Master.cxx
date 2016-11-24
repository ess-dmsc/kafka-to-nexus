#include "Master.h"
#include "NexusWriter.h"
#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;

Master::Master(MasterConfig config) : config(config) {
}

void Master::listen_start() {
	string errstr;
	auto gconf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	gconf->set("metadata.broker.list", config.broker_command_address, errstr);
	auto tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	auto kcons = RdKafka::Consumer::create(gconf, errstr);
	if (not kcons) {
		LOG(3, "ERROR can not create the consumer {}", errstr);
		throw BrokerFailure(errstr);
	}

	auto topic = RdKafka::Topic::create(kcons, config.broker_command_topic, tconf, errstr);
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
}


BrokerFailure::BrokerFailure(std::string msg) : std::runtime_error(msg) { }


}
}
