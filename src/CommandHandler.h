#pragma once

#include "MainOpt.h"
#include "Master.h"
#include <memory>

class T_CommandHandler;

namespace FileWriter {

struct StreamSettings;

/// Given a `Msg` or a JSON command in form of a `std::string` it will
/// interpret and execute the command.

class CommandHandler {
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
  std::vector<StreamHDFInfo>
  initializeHDF(FileWriterTask &Task,
                std::string const &NexusStructureString) const;
  MainOpt &Config;
  Master *MasterPtr = nullptr;
  std::vector<std::unique_ptr<FileWriterTask>> FileWriterTasks;
  friend class ::T_CommandHandler;
};

std::string findBroker(std::string const &);

} // namespace FileWriter
