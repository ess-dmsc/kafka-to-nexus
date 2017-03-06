#pragma once

#include "Master.h"
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/schema.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>

namespace BrightnESS {
namespace FileWriter {

/// Stub, will perform the JSON parsing and then take appropriate action.
class CommandHandler : public FileWriterCommandHandler {
public:
CommandHandler(Master * master);
void handle_new(rapidjson::Document & d);
void handle_exit(rapidjson::Document & d);
void handle(Msg const & msg);
private:
std::unique_ptr<rapidjson::SchemaDocument> schema_command;
Master * master;
std::vector<std::unique_ptr<FileWriterTask>> file_writer_tasks;
};

}
}
