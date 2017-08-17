#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include "utils.h"

namespace FileWriter {
class Streamer;
namespace Status {

class StdIOWriter;
class JSONWriter;
class FlatbuffersWriter;

class StreamerStatus;
class StreamMasterStatus;

// Error codes for the StreamMaster and Streamers
enum StreamMasterErrorCode {
  not_started = 0,
  running = 1,
  has_finished = 2,
  empty_streamer = 3,
  streamer_error = -1,
  statistics_failure = -10,
  streammaster_error = -1000
};
enum StreamerErrorCode {
  no_error = 1001,
  writing = 1,
  stopped = 0,
  configuration_error = -1,
  consumer_error = -2,
  metadata_error = -3,
  topic_partition_error = -4,
  assign_error = -5,
  topic_error = -6,
  offset_error = -7,
  start_time_error = -8,
  message_error = -9,
  write_error = -10,
  not_initialized = -1000,
};

const std::string Err2Str(const FileWriter::StreamMasterError &);
const std::string Err2Str(const FileWriter::StreamerError &);

// Data type for collecting informations about the streamer. `bytes`
// and `messages` store the total number of bytes and messages
// received, and `errors` the total number of errors while processing
// the message, `bytes_squared` and `messages_squared` are the sum of
// the square of the corresponding quantities.  Given these values one
// can compute the average message size (in bytes), the average
// message frequency and their corresponding standard deviation.
class StreamerStatusType {
  friend class StdIOWriter;
  friend class JSONWriter;
  friend class FlatbuffersWriter;

public:
  StreamerStatusType() = default;
  StreamerStatusType(const StreamerStatusType &other);
  StreamerStatusType(StreamerStatusType &&other) noexcept = default;

  ~StreamerStatusType() = default;

  StreamerStatusType &operator=(const StreamerStatusType &other) = default;
  StreamerStatusType &operator=(StreamerStatusType &&other) noexcept;
  StreamerStatusType &operator+=(const StreamerStatusType &other);
  StreamerStatusType &operator-=(const StreamerStatusType &other);
  StreamerStatusType operator+(const StreamerStatusType &other);

  void reset();

  double bytes{0.0};
  double messages{0.0};
  double errors{0.0};
  double bytes_squared{0.0};
  double messages_squared{0.0};
};

// Data type for extracting statistics about the streamer
struct StreamerStatisticsType {
  double average_message_size{0};
  double standard_deviation_message_size{0};
  double average_message_frequency{0};
  double standard_deviation_message_frequency{0};
};

class StreamMasterStatus {
  friend class StdIOWriter;
  friend class JSONWriter;
  friend class FlatbuffersWriter;

public:
  StreamMasterStatus() = default;
  explicit StreamMasterStatus(const int &value) : status(value){};

  StreamMasterStatus &push(const std::string &topic_name,
                           const StreamerStatusType &status,
                           const StreamerStatisticsType &stats) {
    topic.push_back(topic_name);
    streamer_status.push_back(status);
    streamer_stats.push_back(stats);
    return *this;
  }

private:
  std::vector<std::string> topic;
  std::vector<StreamerStatusType> streamer_status;
  std::vector<StreamerStatisticsType> streamer_stats;
  int status{0};
};

// Collects informations about the StreamMaster and the Streamers,
// extract statistics
class StreamerStatus {
  using StreamerError = FileWriter::StreamerError;

  friend class StdIOWriter;
  friend class JSONWriter;
  friend class FlatbuffersWriter;

public:
  StreamerStatus()
      : last_time{std::chrono::system_clock::now()},
        run_status_(StreamerError(StreamerErrorCode::stopped)) {}
  StreamerStatus(const StreamerStatus &other)
      : current{other.current}, last{other.last}, last_time{other.last_time},
        run_status_(other.run_status_) {}

  void add_message(const double &bytes);
  void error() { current.errors++; }
  void error(const StreamerError &value) {
    run_status_ = value;
    error();
  }

  StreamerStatusType fetch_status();

  StreamerStatisticsType fetch_statistics();

  void run_status(const StreamerError &value) { run_status_ = value; }
  const StreamerError &run_status() { return run_status_; }

private:
  StreamerStatusType current;
  StreamerStatusType last;
  std::mutex m;
  std::chrono::system_clock::time_point last_time;
  StreamerError run_status_;
}; // namespace Status

} // namespace Status
} // namespace FileWriter
