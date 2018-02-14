#pragma once

#include "MainOpt.h"
#include "Master.h"
#include <memory>
#include <nlohmann/json.hpp>

class T_CommandHandler;

namespace FileWriter {

struct StreamSettings;

/// Stub, will perform the JSON parsing and then take appropriate action.
class CommandHandler : public FileWriterCommandHandler {
public:
  CommandHandler(MainOpt &config, Master *master);
  void handleNew(std::string const &Command);
  void handleExit();
  void handleFileWriterTaskClearAll();
  void handleStreamMasterStop(std::string const &Command);
  void handle(Msg const &msg);
  void handle(std::string const &command);

private:
  void addStreamSourceToWriterModule(
      const std::vector<StreamSettings> &stream_settings_list,
      std::unique_ptr<FileWriterTask> &fwt);
  MainOpt &Config;
  Master *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
  friend class ::T_CommandHandler;
};

} // namespace FileWriter
