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

namespace BrightnESS {
namespace FileWriter {

struct MasterConfig {
  CommandListenerConfig command_listener;
  uint64_t teamid = 0;
  std::string dir_assets = ".";
  rapidjson::Value const *config_file = nullptr;
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
  void on_consumer_connected(std::function<void(void)> *cb_on_connected);
  std::function<void(void)> cb_on_filewriter_new;

  MasterConfig &config;

private:
  CommandListener command_listener;
  std::atomic<bool> do_run{ true };
  std::function<void(void)> *_cb_on_connected = nullptr;
  std::vector<std::unique_ptr<StreamMaster<Streamer, DemuxTopic> > >
  stream_masters;
  friend class CommandHandler;
};

} // namespace FileWriter
} // namespace BrightnESS
