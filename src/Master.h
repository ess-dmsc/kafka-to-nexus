#pragma once
#include "CommandListener.h"
#include "KafkaW.h"
#include "SchemaRegistry.h"
#include "StreamMaster.hpp"
#include "Streamer.hpp"
#include <atomic>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace FileWriter {

struct MasterConfig {
  CommandListenerConfig command_listener;
  uint64_t teamid = 0;
  std::string dir_assets = ".";
  rapidjson::Value const *config_file = nullptr;
  std::vector<std::pair<std::string, std::string>> kafka;
  std::vector<rapidjson::Document> commands_from_config_file;
};

/// Listens to the Kafka configuration topic.
/// On a new file writing request, creates new nexusWriter instance.
/// Reacts also to stop, and possibly other future commands.
class Master {
public:
  Master(MasterConfig &config);
  void run();
  void stop();
  void handle_command_message(std::unique_ptr<KafkaW::Msg> &&msg);
  void handle_command(rapidjson::Document const &cmd);
  std::function<void(void)> cb_on_filewriter_new;

  MasterConfig &config;

private:
  CommandListener command_listener;
  std::atomic<bool> do_run{true};
  std::vector<std::unique_ptr<StreamMaster<Streamer, DemuxTopic>>>
      stream_masters;
  friend class CommandHandler;
};

} // namespace FileWriter
