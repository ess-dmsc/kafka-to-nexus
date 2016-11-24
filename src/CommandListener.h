#pragma once

#include <thread>
#include <librdkafka/rdkafkacpp.h>

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
void run();
private:
CommandListenerConfig config;
std::thread thr_consumer;
};

}
}
