#pragma once

#include "MainOpt.h"
#include "Master.h"
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>

class T_CommandHandler;

namespace FileWriter {

/// Stub, will perform the JSON parsing and then take appropriate action.
class CommandHandler : public FileWriterCommandHandler {
public:
  CommandHandler(MainOpt &config, Master *master);
  void handle_new(rapidjson::Document const &d);
  void handle_exit(rapidjson::Document const &d);
  void handle(Msg const &msg);
  void handle(rapidjson::Document const &cmd);

private:
  MainOpt &config;
  std::unique_ptr<rapidjson::SchemaDocument> schema_command;
  Master *master = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> file_writer_tasks;
  friend class ::T_CommandHandler;
};

} // namespace FileWriter
