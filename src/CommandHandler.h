#pragma once

#include "MainOpt.h"
#include "Master.h"
#include <memory>
#include <nlohmann/json.hpp>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>

class T_CommandHandler;

namespace FileWriter {

struct StreamSettings;

/// Stub, will perform the JSON parsing and then take appropriate action.
class CommandHandler : public FileWriterCommandHandler {
public:
  CommandHandler(MainOpt &config, Master *master);
  void handleNew(std::string const &command);
  void handleExit(nlohmann::json const &d);
  void handleFileWriterTaskClearAll(nlohmann::json const &d);
  void handleStreamMasterStop(nlohmann::json const &d);
  void handle(Msg const &msg);
  void handle(std::string const &command);

private:
  void addStreamSourceToWriterModule(
      const std::vector<StreamSettings> &stream_settings_list,
      std::unique_ptr<FileWriterTask> &fwt);
  MainOpt &config;
  std::unique_ptr<rapidjson::SchemaDocument> schema_command;
  Master *master = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> file_writer_tasks;
  friend class ::T_CommandHandler;
};

} // namespace FileWriter
