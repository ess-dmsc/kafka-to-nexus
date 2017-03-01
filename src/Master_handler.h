#pragma once

#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include "kafka_util.h"
#include "KafkaW.h"
#include "Msg.h"

namespace BrightnESS {
namespace FileWriter {

/// Abstract the message for testing
class CmdMsg {
public:
virtual ~CmdMsg() {}
virtual std::string & str() = 0;
};

/// Message wrapper
class CmdMsg_K : public CmdMsg {
public:
std::string & str();
std::string _str;
};

/// Master will be responsible for parsing the command messages.

/// Still, abstract away the interface for decoupling:
class FileWriterCommandHandler {
public:
virtual void handle(Msg const & msg) = 0;
};

}
}
