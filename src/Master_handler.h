#pragma once

#include <string>
#include <stdexcept>

namespace BrightnESS {
namespace FileWriter {

/// Master will be responsible for parsing the command messages.
/// Still, abstract away the interface for decoupling:
class FileWriterCommandHandler {
public:
//using CmdJson = std::unique_ptr<rapidjson::Document>;
void handle_command(std::unique_ptr<RdKafka::Message> msg);
};

}
}
