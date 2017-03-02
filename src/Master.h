#pragma once
#include <string>
#include <stdexcept>
#include <vector>
#include <functional>
#include <atomic>
#include "SchemaRegistry.h"
#include "CommandListener.h"
#include "NexusWriter.h"
#include "StreamMaster.hpp"
#include "Streamer.hpp"
#include "KafkaW.h"

namespace BrightnESS {
namespace FileWriter {

struct MasterConfig {
CommandListenerConfig command_listener;
uint64_t teamid = 0;
std::string dir_assets = ".";
};

/// Listens to the Kafka configuration topic.
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
Master(MasterConfig config);
void run();
void stop();
void handle_command_message(std::unique_ptr<KafkaW::Msg> && msg);
void on_consumer_connected(std::function<void(void)> * cb_on_connected);
std::function<void(void)> cb_on_filewriter_new;

MasterConfig config;
private:
CommandListener command_listener;
std::atomic<bool> do_run {true};
std::vector<NexusWriter_uptr> nexus_writers;
std::function<void(void)> * _cb_on_connected = nullptr;
std::vector< std::unique_ptr< StreamMaster<Streamer, DemuxTopic> > > stream_masters;

friend class CommandHandler;
};


}
}
