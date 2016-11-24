#pragma once
#include <string>
#include <stdexcept>

namespace BrightnESS {
namespace FileWriter {

// POD
struct MasterConfig {
std::string broker_command_address = "localhost:9092";
std::string broker_command_topic = "ess-file-writer.command";
};


/// Listens to the Kafka configuration topic.
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
Master(MasterConfig config);

/// Start listening to command messages
void listen_start();
void listen_stop();

private:
MasterConfig config;
};


class BrokerFailure : public std::runtime_error {
public:
BrokerFailure(std::string msg);
};

}
}
