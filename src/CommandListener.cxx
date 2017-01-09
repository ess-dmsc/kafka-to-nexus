#include "CommandListener.h"
#include <string>
#include <vector>
#include <map>
#include "logger.h"
#include "helper.h"
#include "Master_handler.h"
#include "kafka_util.h"
#include <cassert>
#include <sys/types.h>
#include <unistd.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <rapidjson/prettywriter.h>


namespace BrightnESS {
namespace FileWriter {

using std::vector;
using std::string;




#define KERR(err) if (err != 0) { LOG(3, "Kafka error code: {}", err); }


// Kafka Consumer

Consumer::Consumer(BrokerOpt opt) : opt(opt), poll_timeout_ms(100) {
}


Consumer::~Consumer() {
	if (plist) {
		rd_kafka_topic_partition_list_destroy(plist);
		plist = nullptr;
	}
	if (rk) {
		// commit offsets
		rd_kafka_consumer_close(rk);
		rd_kafka_destroy(rk);
		rk = nullptr;
	}
}


static void kafka_log_cb(rd_kafka_t const * rk, int level, char const * fac, char const * buf) {
	LOG(level, "{}  fac: {}", buf, fac);
}

// Called from the poll() thread
static void kafka_error_cb(rd_kafka_t * rk, int err_i, const char * reason, void * opaque) {
	// cast necessary because of Kafka API design
	rd_kafka_resp_err_t err = (rd_kafka_resp_err_t) err_i;
	LOG(7, "ERROR Kafka Config: {}, {}, {}, {}", err_i, rd_kafka_err2name(err), rd_kafka_err2str(err), reason);
	// Could do something with this opaque:
	auto self = static_cast<Consumer*>(opaque);
}



static int stats_cb(rd_kafka_t * rk, char * json, size_t json_len, void * opaque) {
	//LOG(3, "INFO stats_cb length {}", json_len);
	// TODO
	// What does Kafka want us to return from this callback?
	return 0;
}



static void print_partition_list(rd_kafka_topic_partition_list_t * plist) {
	for (int i1 = 0; i1 < plist->cnt; ++i1) {
		auto & x = plist->elems[i1];
		LOG(3, "   {}  {}  {}", x.topic, x.partition, x.offset);
	}
}


static void rebalance_cb(
	rd_kafka_t *rk,
	rd_kafka_resp_err_t err,
	rd_kafka_topic_partition_list_t * plist,
	void *opaque)
{
	rd_kafka_resp_err_t err2;
	LOG(3, "Consumer group rebalanced:");
	switch (err) {
	case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		LOG(3, "rebalance_cb assign:");
		print_partition_list(plist);
		err2 = rd_kafka_assign(rk, plist);
		break;
	case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		LOG(3, "rebalance_cb revoke:");
		print_partition_list(plist);
		err2 = rd_kafka_assign(rk, NULL);
		break;
	default:
		LOG(3, "rebalance_cb failure and revoke: {}", rd_kafka_err2str(err));
		err2 = rd_kafka_assign(rk, NULL);
		break;
	}
}



static void consume_cb(rd_kafka_message_t * msg, void * opaque) {
	auto const & consumer = static_cast<Consumer*>(opaque);

	if (msg) {
		auto topic_name = rd_kafka_topic_name(msg->rkt);
		int partition = msg->partition;
		if (msg->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
			LOG(3, "GOT MESSAGE: {:.{}}", (char*)msg->payload, msg->len);
		}
		else if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			// Just an advisory.  msg contains which partition it is.
		}
		else if (msg->err == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) {
			LOG(3, "RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN");
			return;
		}
		else if (msg->err == RD_KAFKA_RESP_ERR__BAD_MSG) {
			LOG(3, "RD_KAFKA_RESP_ERR__BAD_MSG");
			throw std::runtime_error("RD_KAFKA_RESP_ERR__BAD_MSG");
		}
		else if (msg->err == RD_KAFKA_RESP_ERR__DESTROY) {
			LOG(3, "RD_KAFKA_RESP_ERR__DESTROY");
			// Broker will go away soon
			LOG(3, "WARNING broker will go away");
		}
		else {
			LOG(3, "ERROR unhandled msg error: {} {}", rd_kafka_err2name(msg->err), rd_kafka_err2str(msg->err));
			throw std::runtime_error("unhandled error");
		}
	}
}



void Consumer::start() {
	int err;
	// librdkafka API sometimes wants to write errors into a buffer:
	int const errstr_N = 512;
	char errstr[errstr_N];

	auto conf = rd_kafka_conf_new();

	rd_kafka_conf_set_log_cb(conf, kafka_log_cb);
	rd_kafka_conf_set_error_cb(conf, kafka_error_cb);

	std::map<std::string, int> conf_ints {
		/*
		Default config should be good enough for us.
		{"statistics.interval.ms",                   20 * 1000},
		{"metadata.request.timeout.ms",              15 * 1000},
		{"socket.timeout.ms",                         4 * 1000},
		{"session.timeout.ms",                       15 * 1000},

		{"message.max.bytes",                 23 * 1024 * 1024},
		{"fetch.message.max.bytes",           23 * 1024 * 1024},
		{"receive.message.max.bytes",   5*    23 * 1024 * 1024},
		{"queue.buffering.max.messages",              2 * 1024},
		{"queue.buffering.max.ms",                        2000},
		{"batch.num.messages",                      100 * 1000},
		{"socket.send.buffer.bytes",          23 * 1024 * 1024},
		{"socket.receive.buffer.bytes",       23 * 1024 * 1024},

		// Consumer
		//{"queued.min.messages", "1"},
		*/
	};
	std::map<std::string, std::string> conf_strings {
		{"group.id", "some-group-id"},
	};

	for (auto & c : conf_ints) {
		if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(conf, c.first.c_str(), fmt::format("{:d}", c.second).c_str(), errstr, errstr_N)) {
			LOG(7, "error setting config: {}", c.first.c_str());
		}
	}
	for (auto & c : conf_strings) {
		if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(conf, c.first.c_str(), c.second.c_str(), errstr, errstr_N)) {
			LOG(7, "error setting config: {}", c.first.c_str());
		}
	}

	// TODO
	// Release this resource later:
	//rd_kafka_topic_conf_t * topic_conf = nullptr;
	auto topic_conf = rd_kafka_topic_conf_new();
	//rd_kafka_topic_conf_set(topic_conf, "produce.offset.report", "true", errstr, errstr_N);
	//rd_kafka_topic_conf_set(topic_conf, "message.timeout.ms", "2000", errstr, errstr_N);
	//rd_kafka_topic_conf_set(topic_conf, "offset.store.method", "broker", errstr, errstr_N);

	rd_kafka_conf_set_default_topic_conf(conf, topic_conf);

	//rd_kafka_conf_set_dr_msg_cb(conf, msg_delivered_cb);
	//rd_kafka_conf_set_stats_cb(conf, stats_cb);
	rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

	// This set is used with rd_kafka_consumer_poll together with the opaque
	rd_kafka_conf_set_opaque(conf, this);
	rd_kafka_conf_set_consume_cb(conf, consume_cb);


	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, errstr_N);
	if (!rk) {
		LOG(7, "ERROR can not create kafka handle: {}", errstr);
		throw std::runtime_error("can not create Kafka handle");
	}

	LOG(3, "Name of the new Kafka handle: {}", rd_kafka_name(rk));

	int const LOG_DEBUG = 7;
	rd_kafka_set_log_level(rk, LOG_DEBUG);

	if (rd_kafka_brokers_add(rk, opt.address.c_str()) == 0) {
		LOG(7, "ERROR could not add brokers");
		throw std::runtime_error("could not add brokers");
	}

	int partition = RD_KAFKA_PARTITION_UA;
	plist = rd_kafka_topic_partition_list_new(1);
	rd_kafka_topic_partition_list_add(plist, opt.topic.c_str(), partition);
	rd_kafka_topic_partition_list_set_offset(plist, opt.topic.c_str(), partition, RD_KAFKA_OFFSET_BEGINNING);

	err = rd_kafka_subscribe(rk, plist);
	KERR(err);
	if (err) {
		LOG(7, "ERROR could not subscribe");
		throw std::runtime_error("can not subscribe");
	}
}


void Consumer::dump_current_subscription() {
	// Dump current subscription:
	rd_kafka_topic_partition_list_t * l1 = nullptr;
	rd_kafka_subscription(rk, &l1);
	if (l1) {
		for (int i1 = 0; i1 < l1->cnt; ++i1) {
			LOG(1, "subscribed topics: {}  {}  off {}", l1->elems[i1].topic, rd_kafka_err2str(l1->elems[i1].err), l1->elems[i1].offset);
		}
		rd_kafka_topic_partition_list_destroy(l1);
	}
}





PollStatus Consumer::poll() {
	if (false) {
		dump_current_subscription();
	}

	if (false) {
		rd_kafka_dump(stdout, rk);
	}

	if (true) {
		// only do this if not redirected
		int const timeout = 0;
		int n1 = rd_kafka_poll(rk, timeout);
		int level = 0;
		if (n1 > 0) level = 1;
		LOG(level, "rd_kafka_poll served {} events", n1);
	}

	// max messages consumed in one callback invocation is tuned per topic in "consume.callback.max.messages", default is 0 == inf
	auto msg = rd_kafka_consumer_poll(rk, poll_timeout_ms);
	return PollStatus::OK;
}











CommandListener::CommandListener(CommandListenerConfig config) : config(config) { }

void CommandListener::start() {
	if (is_mockup) {
		LOG(1, "is_mockup, no Kafka init");
		return;
	}

	if (true) {
		// legacy API version
		BrokerOpt opt;
		opt.address = config.address;
		opt.topic = config.topic;
		// TODO
		// Refactor RAAI after it passes tests:
		leg_consumer.reset(new Consumer(opt));
		leg_consumer->start();
	}


	if (false) {
		// C++ Kafka API version
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

		//print_subscribed();
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
	if (is_mockup) {
		LOG(1, "is_mockup, no Kafka");
		auto msg = make_unique<CmdMsg_Mockup>();
		msg->data_ = gulp("test/msg-conf-new-01.json");
		command_handler.handle(std::move(msg));
		return;
	}

	if (true) {
		// legacy API
		leg_consumer->poll();
	}

	if (false) {
		// C++ API
		print_subscribed();
		// Currently, run command-listener single-threaded
		int timeout_ms = 100;
		//LOG(3, "polling");
		auto msg = kcons->consume(timeout_ms);
		auto err = msg->err();
		if (err == RdKafka::ERR_NO_ERROR) {
			LOG(3, "GOT MESSAGE");
		}
		else if (err == RdKafka::ERR__TIMED_OUT) {
			//LOG(9, "ERR__TIMED_OUT");
		}
		else if (err == RdKafka::ERR__PARTITION_EOF) {
			//LOG(9, "ERR__PARTITION_EOF");
			// when topic is empty, poll results most of the time in a timeout,
			// so how is this error code anymore useful than just a timeout?
		}
		else {
			LOG(9, "ERROR while polling for messages: {}", RdKafka::err2str(err));
		}
	}
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

	void * msg_data = nullptr;
	int msg_size = 0;

	auto v1 = gulp("test/msg-conf-new-01.json");
	msg_data = v1.data();
	msg_size = v1.size();

	RdKafka::ErrorCode err;
	err = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, msg_data, msg_size, nullptr, nullptr);
	if (err != RdKafka::ERR_NO_ERROR) {
		auto e = "ERROR produce gave error";
		LOG(3, e);
		throw BrokerFailure(e);
	}
}


}
}
