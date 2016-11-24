#pragma once

#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include "Master_handler.h"

namespace BrightnESS {
namespace FileWriter {

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

/// Only used for testing:
bool is_mockup = false;
private:
CommandListenerConfig config;
std::thread thr_consumer;
std::unique_ptr<RdKafka::Conf> gconf;
std::unique_ptr<RdKafka::Conf> tconf;
std::unique_ptr<RdKafka::Consumer> kcons;
std::unique_ptr<RdKafka::Topic> topic;
int32_t partition = RdKafka::Topic::PARTITION_UA;
};

}
}
