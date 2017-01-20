#pragma once
#include <string>
#include <stdexcept>
#include <vector>
#include <functional>
#include <atomic>
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
void handle_command_message(std::unique_ptr<CmdMsg> && msg);
void on_consumer_connected(std::function<void(void)> * cb_on_connected);
std::function<void(void)> cb_on_filewriter_new;

private:
MasterConfig config;
CommandListener command_listener;
std::atomic<bool> do_run {true};
std::vector<NexusWriter_uptr> nexus_writers;
std::function<void(void)> * _cb_on_connected = nullptr;

friend class CommandHandler;
};


}
}
