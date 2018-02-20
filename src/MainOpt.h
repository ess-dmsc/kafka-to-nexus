#pragma once

#include "Alloc.h"
#include "StreamerOptions.h"
#include "logger.h"
#include "uri.h"

#include <atomic>
#include <chrono>
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
class StreamerOptions;
} // namespace FileWriter

// POD
struct MainOpt {
  void init();
  bool help = false;
  bool gtest = false;
  bool use_signal_handler = true;
  /// Kafka options, they get parsed from the configuration file and passed on
  /// to the StreamMaster.
  std::map<std::string, std::string> kafka;
  /// Streamer options, they get parsed from the configuration file and passed
  /// on to the StreamMaster.
  FileWriter::StreamerOptions StreamerConfiguration;
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
  std::vector<std::string> commands_from_config_file;
  /// Called on startup when a `--config-file` is found.
  int parse_config_file(std::string fname);
  /// Used in turn by `parse_config_file` to parse the json data.
  int parse_config_json(std::string json);
  /// Kafka broker and topic where file writer commands are published.
  uri::URI command_broker_uri{"kafka://localhost:9092/kafka-to-nexus.command"};
  /// Path for HDF output. This gets prepended to the HDF output filename given
  /// in the write commands.
  std::string hdf_output_prefix;

  /// Whether we want to publish status to Kafka
  bool do_kafka_status = false;
  /// Kafka topic where status updates are to be published
  uri::URI kafka_status_uri{"kafka://localhost:9092/kafka-to-nexus.status"};
  /// std::chrono::milliseconds interval to publish status of `Master` (e.g.
  /// list of current file writings)
  uint32_t status_master_interval = 2000;

  // For testing.
  std::function<void(rd_kafka_topic_partition_list_s *)> on_rebalance_assign;
  // For testing.
  int64_t start_at_command_offset = -1;
  /// Was/is used for testing during development.
  uint64_t teamid = 0;
  bool source_do_process_message = true;
  Alloc::sptr jm;
  bool logpid_sleep = false;
  // Max interval (in std::chrono::milliseconds) to spend writing each topic
  // before switch to the next
  std::chrono::milliseconds topic_write_duration;
};

std::pair<int, std::unique_ptr<MainOpt>> parse_opt(int argc, char **argv);
void setup_logger_from_options(MainOpt const &opt);
extern std::atomic<MainOpt *> g_main_opt;
