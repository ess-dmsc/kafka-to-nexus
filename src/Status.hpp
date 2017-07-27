#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

namespace FileWriter {
class Streamer;
namespace Status {

// class DummyWriter {
// }

class StdIOWriter;
class JSONWriter;

class StreamerStatus;
class StreamMasterStatus;

class StreamerStatusType {
  friend class StdIOWriter;
  friend class CJSONWriter;
  //  friend void pprint(const StreamerStatusType &);

public:
  StreamerStatusType();
  StreamerStatusType(const StreamerStatusType &other);
  StreamerStatusType(StreamerStatusType &&other) noexcept = default;

  ~StreamerStatusType() = default;

  StreamerStatusType &operator=(const StreamerStatusType &other);
  StreamerStatusType &operator=(StreamerStatusType &&other);
  StreamerStatusType &operator+=(const StreamerStatusType &other);
  StreamerStatusType &operator-=(const StreamerStatusType &other);
  StreamerStatusType operator+(const StreamerStatusType &other);

  void reset();

  double bytes;
  double messages;
  double errors;
  double bytes2;
  double messages2;
};

struct StreamerStatisticsType {
  double size_avg{0}, size_std{0};
  double freq_avg{0}, freq_std{0};
  int partitions{0};
};

enum RunStatusError {
  running = 1,
  stopped = 0,
  consumer_error = -1,
  metadata_error = -2,
  topic_partition_error = -3,
  assign_error = -4,
  topic_error = -5,
  offset_error = -6,
  start_time_error = -7,
  streammaster_error = -1000
};

class StreamMasterStatus {
  //  friend void pprint(const StreamMasterStatus &);
  friend class StdIOWriter;
  friend class CJSONWriter;

public:
  StreamMasterStatus() = default;
  StreamMasterStatus(const int &value) : status(value){};

  StreamMasterStatus &push(const std::string topic_name,
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

class StreamerStatus {
  friend class StdIOWriter;
  friend class CJSONWriter;

public:
  StreamerStatus()
      : last_time{std::chrono::system_clock::now()}, partitions{0},
        run_status_(RunStatusError::stopped) {}
  StreamerStatus(const StreamerStatus &other)
      : current{other.current}, last{other.last}, last_time{other.last_time},
        partitions{other.partitions}, run_status_(other.run_status_) {}

  void add_message(const double &bytes);
  void error() { current.errors++; }

  StreamerStatusType fetch_status();

  StreamerStatisticsType fetch_statistics();

  void run_status(const int8_t &value) { run_status_ = value; }

private:
  StreamerStatusType current;
  StreamerStatusType last;
  std::mutex m;
  std::chrono::system_clock::time_point last_time;
  uint32_t partitions;
  int8_t run_status_;
}; // namespace Status

// template <typename W> typename W::return_type pprint(StreamMasterStatus &);
// template <> typename JSONWriter::return_type pprint<JSONWriter>(StreamMasterStatus &);
// template <> typename StdIOWriter::return_type pprint<StdIOWriter>(StreamMasterStatus &);

} // namespace Status
} // namespace FileWriter
