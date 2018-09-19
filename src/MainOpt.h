#pragma once

#include "StreamerOptions.h"
#include "json.h"
#include "logger.h"
#include "uri.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

struct rd_kafka_topic_partition_list_s;

// POD
struct MainOpt {
  bool help = false;
  bool gtest = false;
  bool use_signal_handler = true;
  /// Each running filewriter should be identifiable by some id.
  /// This service_id is announced in the status updates.
  /// It is by default a combination of hostname and process id.
  /// Can be set via command line or configuration file.
  std::string service_id;
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
  /// The configuration file given by the `--config-file` option.
  nlohmann::json ConfigJSON = nlohmann::json::object();
  /// The configuration filename given by the `--config-file` option.
  std::string config_filename;
  /// Keeps commands contained in the configuration file.  The configuration
  /// file may contain commands which are executed before any other command
  /// from the Kafka command topic.
  std::vector<std::string> commands_from_config_file;
  /// Called on startup when a `--config-file` is found.
  int parse_config_file();
  /// Used in turn by `parse_config_file` to parse the json data.
  int parse_config_json(std::string json);
  /// Kafka broker and topic where file writer commands are published.
  uri::URI command_broker_uri{"kafka://localhost:9092/kafka-to-nexus.command"};
  /// Path for HDF output. This gets prepended to the HDF output filename given
  /// in the write commands.
  std::string hdf_output_prefix;

  /// Used for command line argument.
  bool ListWriterModules{false};

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
  bool logpid_sleep = false;
  // Max interval (in std::chrono::milliseconds) to spend writing each topic
  // before switch to the next
  std::chrono::milliseconds topic_write_duration;
  // The constructor was removed because of the issue with the integration test
  // (see cpp file for more details).
  void init();
};

void setup_logger_from_options(MainOpt const &opt);
