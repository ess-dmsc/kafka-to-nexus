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
  /// Kafka options, they get parsed from the configuration file and passed on
  /// to the StreamMaster.
  std::map<std::string, std::string> kafka;
  /// Can optionally log in Graylog GELF format to a Kafka topic.
  std::string kafka_gelf;
  /// Can optionally use the `graylog_logger` library to log to this address.
  std::string graylog_logger_address;
  /// Used by the signal handler.
  std::atomic<FileWriter::Master *> master{nullptr};
  /// The parsed configuration file given by the `--config-file` option.
  rapidjson::Document config_file;
  /// Keeps commands contained in the configuration file.  The configuration
  /// file may contain commands which are executed before any other command
  /// from the Kafka command topic.
  std::vector<rapidjson::Document> commands_from_config_file;
  /// Called on startup when a `--config-file` is found.
  int parse_config_file(std::string fname);
  /// Kafka broker and topic where file writer commands are published.
  uri::URI command_broker_uri{"kafka://localhost:9092/kafka-to-nexus.command"};

  // For testing.
  std::function<void(rd_kafka_topic_partition_list_s *)> on_rebalance_assign;
  // For testing.
  int64_t start_at_command_offset = -1;
  /// Was/is used for testing during development.
  uint64_t teamid = 0;
};

std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char **argv);
void setup_logger_from_options(MainOpt const &opt);
extern std::atomic<MainOpt *> g_main_opt;
