#pragma once
#include <string>
#include <stdexcept>
#include "CommandListener.h"

namespace BrightnESS {
namespace FileWriter {

struct MasterConfig {
CommandListenerConfig command_listener;
};

/// Listens to the Kafka configuration topic.
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
Master(MasterConfig config);

private:
MasterConfig config;
CommandListener command_listener;
};


}
}
