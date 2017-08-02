#pragma once

#include "logger.h"
#include "uri.h"
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <utility>
#include <vector>

struct rd_kafka_topic_partition_list_s;

namespace FileWriter {
class Master;
}

// POD
struct MainOpt {
  bool help = false;
  bool verbose = false;
  bool gtest = false;
  bool use_signal_handler = true;
  // FileWriter::MasterConfig master_config;
  std::map<std::string, std::string> kafka;
  std::string kafka_gelf;
  std::string graylog_logger_address;
  std::atomic<FileWriter::Master *> master;
  rapidjson::Document config_file;
  // rapidjson::Value const *config_file = nullptr;

  std::vector<rapidjson::Document> commands_from_config_file;
  int parse_config_file(std::string fname);
  // CommandListenerConfig command_listener;
  uint64_t teamid = 0;

  uri::URI command_broker_uri{"kafka://localhost:9092/kafka-to-nexus.command"};
  std::function<void(rd_kafka_topic_partition_list_s *)> on_rebalance_assign;
  int64_t start_at_command_offset = -1;
};

std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char **argv);
void setup_logger_from_options(MainOpt const &opt);
extern std::atomic<MainOpt *> g_main_opt;
