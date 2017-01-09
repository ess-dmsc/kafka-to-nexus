#pragma once

#include <thread>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include "Master_handler.h"

namespace BrightnESS {
namespace FileWriter {



class BrokerOpt {
public:
std::string address = "localhost:9092";
std::string topic = "ess-file-writer.command";
};


class MessageCallback {
public:
virtual void operator() (int partition, std::string const & topic, std::string const & msg) = 0;
};


enum class PollStatus {
	OK,
};


class Consumer {
public:
Consumer(BrokerOpt opt);
~Consumer();
void start();
void dump_current_subscription();
PollStatus poll();

private:
BrokerOpt opt;
int poll_timeout_ms = 10;
static void log_cb(rd_kafka_t const * rk, int level, char const * fac, char const * buf);
rd_kafka_t * rk = nullptr;
//rd_kafka_topic_t * rkt = nullptr;
rd_kafka_topic_partition_list_t * plist = nullptr;
};




/// Settings for the Kafka command broker and topic.
struct CommandListenerConfig {
std::string address = "localhost:9092";
std::string topic = "ess-file-writer.command";
};


/// Check for new commands on the topic, dispatch to the Master
class CommandListener {
public:
CommandListener(CommandListenerConfig);
/// Start listening to command messages
void start();
void stop();
/// Check for new command packets
void poll(FileWriterCommandHandler & command_handler);
void print_subscribed();

/// Only used for testing:
bool is_mockup = false;
private:
CommandListenerConfig config;
std::thread thr_consumer;
std::unique_ptr<RdKafka::Conf> gconf;
std::unique_ptr<RdKafka::Conf> tconf;
std::unique_ptr<RdKafka::KafkaConsumer> kcons;
std::unique_ptr<RdKafka::Topic> topic;
int32_t partition = RdKafka::Topic::PARTITION_UA;

std::unique_ptr<Consumer> leg_consumer;
};


/// Produce pre-fabricated commands for testing
class TestCommandProducer {
public:
/// Just a preliminary name for a first test command
void produce_simple_01(CommandListenerConfig config);
};

}
}
