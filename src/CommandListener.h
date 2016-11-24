#pragma once

#include <thread>
#include <librdkafka/rdkafkacpp.h>
#include "Master_handler.h"

namespace BrightnESS {
namespace FileWriter {

// POD
struct CommandListenerConfig {
std::string address = "localhost:9092";
std::string topic = "ess-file-writer.command";
};


class CommandListener {
public:
CommandListener(CommandListenerConfig);
/// Start listening to command messages
void start();
void stop();
void poll(FileWriterCommandHandler & command_handler);

// Only used for testing:
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
