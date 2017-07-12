#pragma once

#include "KafkaW.h"
#include "Msg.h"
#include "kafka_util.h"
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace FileWriter {

/// Abstract the message for testing
class CmdMsg {
public:
  virtual ~CmdMsg() {}
  virtual std::string &str() = 0;
};

/// Message wrapper
class CmdMsg_K : public CmdMsg {
public:
  std::string &str();
  std::string _str;
};

/// Master will be responsible for parsing the command messages.

/// Still, abstract away the interface for decoupling:
class FileWriterCommandHandler {
public:
  virtual void handle(Msg const &msg) = 0;
};

} // namespace FileWriter
