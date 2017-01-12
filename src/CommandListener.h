#pragma once

#include <thread>
#include "Master_handler.h"

namespace BrightnESS {
namespace FileWriter {



class BrokerOpt {
public:
std::string address = "localhost:9092";
std::string topic = "ess-file-writer.command";
};


class MessageCallback {
public:
virtual void operator() (int partition, std::string const & topic, std::string const & msg) = 0;
};




/// Settings for the Kafka command broker and topic.
struct CommandListenerConfig {
std::string address = "localhost:9092";
std::string topic = "ess-file-writer.command";
std::function<void()> * on_rebalance_assign = nullptr;
int64_t start_at_command_offset = -1;
};

class Consumer;

class PollStatus {
public:
static PollStatus Ok();
static PollStatus Err();
static PollStatus make_CmdMsg(std::unique_ptr<CmdMsg> x);
PollStatus(PollStatus &&);
PollStatus & operator = (PollStatus &&);
~PollStatus();
void reset();
PollStatus();
bool is_Ok();
bool is_Err();
std::unique_ptr<CmdMsg> is_CmdMsg();
private:
int state = -1;
void * data = nullptr;
};


/// Check for new commands on the topic, dispatch to the Master
class CommandListener {
public:
CommandListener(CommandListenerConfig);
~CommandListener();
/// Start listening to command messages
void start();
void stop();
/// Check for new command packets
PollStatus poll(FileWriterCommandHandler & command_handler);

/// Only used for testing:
bool is_mockup = false;
private:
CommandListenerConfig config;
std::thread thr_consumer;
std::unique_ptr<Consumer> leg_consumer;
};


/// Produce pre-fabricated commands for testing
class TestCommandProducer {
public:
/// Just a preliminary name for a first test command
int64_t produce_simple_01(CommandListenerConfig config);
};

}
}
