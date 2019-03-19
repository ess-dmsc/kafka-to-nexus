#pragma once

#include "StreamerOptions.h"
#include "URI.h"
#include "json.h"
#include "logger.h"

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
  bool gtest = false;
  bool use_signal_handler = true;

  /// \brief Each running filewriter is identifiable by an id.
  ///
  /// This `service_id` is announced in the status updates.
  /// It is by default a combination of hostname and process id.
  /// Can be set via command line or configuration file.
  std::string ServiceID;

  /// \brief Streamer options are parsed from the configuration file and passed
  /// on to the StreamMaster.
  FileWriter::StreamerOptions StreamerConfiguration;

  spdlog::level::level_enum LoggingLevel;

  /// Can optionally use the `graylog_logger` library to log to this address.
  std::string GraylogLoggerAddress;

  /// Used for logging to file
  std::string LogFilename;

  /// The commands file given by the `--commands-json` option.
  nlohmann::json CommandsJson = nlohmann::json::object();

  /// The commands filename given by the `--commands-json` option.
  std::string CommandsJsonFilename;

  /// \brief Holds commands from the configuration file.
  ///
  /// The configuration file may contain commands which are executed before any
  /// other command from the Kafka command topic.
  std::vector<std::string> CommandsFromJson;

  /// Called on startup when a `--commands-json` is found.
  int parseJsonCommands();

  /// Kafka broker and topic where file writer commands are published.
  uri::URI CommandBrokerURI{"//localhost:9092/kafka-to-nexus.command"};

  /// \brief Path for HDF output.
  ///
  /// This gets prepended to the HDF output filename given in the write
  /// commands.
  std::string HDFOutputPrefix;

  /// Used for command line argument.
  bool ListWriterModules = false;

  /// Whether we want to publish status to Kafka.
  bool ReportStatus = false;

  /// Kafka topic where status updates are to be published.
  uri::URI KafkaStatusURI{"//localhost:9092/kafka-to-nexus.status"};

  /// \brief std::chrono::milliseconds interval to publish status of `Master`
  /// (e.g.
  /// list of current file writings).
  uint32_t StatusMasterIntervalMS = 2000;

  // For testing.
  std::function<void(rd_kafka_topic_partition_list_s *)> on_rebalance_assign;
  // For testing.
  int64_t start_at_command_offset = -1;
  // Was/is used for testing during development.
  uint64_t teamid = 0;
  bool logpid_sleep = false;

  /// \brief Max interval (in std::chrono::milliseconds) to spend writing each
  /// topic before switch to the next.
  std::chrono::milliseconds topic_write_duration;

  // The constructor was removed because of the issue with the integration test
  // (see cpp file for more details).
  void init();
  void findAndAddCommands();
};

void setupLoggerFromOptions(MainOpt const &opt);
