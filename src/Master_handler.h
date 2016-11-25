#pragma once

#include <string>
#include <stdexcept>
#include "kafka_util.h"

namespace BrightnESS {
namespace FileWriter {

/// Abstract the message for testing
class CmdMsg {
public:
/// Returns pointer into the underlying message buffer
virtual char * data() = 0;
/// Number of bytes in the message
virtual size_t size() = 0;
};

/// The real Kafka one
class CmdMsg_K : public CmdMsg {
private:
std::unique_ptr<RdKafka::Message> msg_k;
};

/// For testing
class CmdMsg_Mockup : public CmdMsg {
public:
char * data() { return data_.data(); }
size_t size() { return data_.size(); }
/// Mockup data to be filled by the testing code
std::vector<char> data_;
};


/// Master will be responsible for parsing the command messages.

/// Still, abstract away the interface for decoupling:
class FileWriterCommandHandler {
public:
virtual void handle(std::unique_ptr<CmdMsg> msg) = 0;
};

}
}
