#pragma once
#include <string>
#include <stdexcept>
#include "CommandListener.h"

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

private:
MasterConfig config;
CommandListener command_listener;
};


}
}
