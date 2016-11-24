#pragma once
#include <string>

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
Master();
};

}
}
