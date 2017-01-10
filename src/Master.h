#pragma once
#include <string>
#include <stdexcept>
#include <vector>
#include "CommandListener.h"
#include "NexusWriter.h"

namespace BrightnESS {
namespace FileWriter {

struct MasterConfig {
CommandListenerConfig command_listener;
bool test_mockup_command_listener {false};
};

/// Listens to the Kafka configuration topic.
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
Master(MasterConfig config);
void run();
void on_consumer_connected(std::function<void(void)> const & cb_on_connected);

private:
MasterConfig config;
CommandListener command_listener;
std::vector<NexusWriter_uptr> nexus_writers;
std::function<void(void)> const * _cb_on_connected = nullptr;

friend class CommandHandler;
};


}
}
