#include "CommandListener.h"
#include <string>
#include "logger.h"
#include "helper.h"
#include "Master_handler.h"
#include "kafka_util.h"
#include <cassert>
#include <sys/types.h>
#include <unistd.h>


namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;

CommandListener::CommandListener(CommandListenerConfig config) : config(config) { }

void CommandListener::start() {
	if (is_mockup) {
		LOG(1, "is_mockup, no Kafka init");
		return;
	}
	string errstr;
	gconf = decltype(gconf)(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	gconf->set("metadata.broker.list", config.address, errstr);
	if (errstr.size() > 0) {
		// yeah, seriously....
		LOG(3, "errstr: {}", errstr);
		throw BrokerFailure(errstr);
		errstr.clear();
	}
	auto unique_group_id = fmt::format("{}", getpid());
	gconf->set("group.id", unique_group_id, errstr);
	tconf = decltype(tconf)(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
	kcons = decltype(kcons)(RdKafka::KafkaConsumer::create(gconf.get(), errstr));
	if (not kcons) {
		LOG(3, "ERROR can not create the consumer {}", errstr);
		throw BrokerFailure(errstr);
	}

	/*
	topic = decltype(topic)(RdKafka::Topic::create(kcons.get(), config.topic, tconf.get(), errstr));
	if (not topic) {
		LOG(3, "ERROR can not create the topic: {}", errstr);
		throw BrokerFailure(errstr);
	}
	*/

	vector<string> topics = { config.topic };
	auto err = kcons->subscribe(topics);
	if (err != RdKafka::ERR_NO_ERROR) {
		LOG(3, "ERROR can not subscribe with KafkaConsumer {}", errstr);
		throw BrokerFailure(errstr);
	}

	print_subscribed();
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
	if (is_mockup) {
		LOG(1, "is_mockup, no Kafka");
		auto msg = make_unique<CmdMsg_Mockup>();
		msg->data_ = gulp("test/msg-conf-new-01.json");
		command_handler.handle(std::move(msg));
		return;
	}

	print_subscribed();
	// Currently, run command-listener single-threaded
	int timeout_ms = 10;
	kcons->consume(timeout_ms);
}


void CommandListener::print_subscribed() {
	vector<RdKafka::TopicPartition*> topic_partitions;
	auto err = kcons->assignment(topic_partitions);
	string errstr;
	if (err != RdKafka::ERR_NO_ERROR) {
		LOG(3, "ERROR can not start Consumer {}", errstr);
		throw BrokerFailure(errstr);
	}
	LOG(3, "Currently subscribed to:");
	for (auto & tp : topic_partitions) {
		LOG(3, "Topic: {}  Partition: {}  Offset: {}", tp->topic(), tp->partition(), tp->offset());
	}
}



void TestCommandProducer::produce_simple_01(CommandListenerConfig config) {
	LOG(3, "Use for configuration the topic {}", config.topic);
	string errstr;
	auto gconf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	gconf->set("metadata.broker.list", config.address, errstr);

	auto producer = RdKafka::Producer::create(gconf, errstr);
	auto topic = RdKafka::Topic::create(producer, config.topic, nullptr, errstr);
	if (errstr.size() > 0) {
		auto e = "ERROR can not create topic";
		LOG(9, e);
		throw BrokerFailure(e);
	}

	string msg("MESSAGE01");

	RdKafka::ErrorCode err;
	err = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, (void*)msg.data(), msg.size(), nullptr, nullptr);
	if (err != RdKafka::ERR_NO_ERROR) {
		auto e = "ERROR produce gave error";
		LOG(3, e);
		throw BrokerFailure(e);
	}
}


}
}
