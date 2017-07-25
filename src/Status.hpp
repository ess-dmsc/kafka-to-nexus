#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <mutex>

namespace FileWriter {
class Streamer;
namespace Status {

struct StreamerStatusType {
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

class StreamMasterStatus {
  enum {
    running = 1,
    stopped = 0,
    streammaster_error = -1,
    streamer_error = -2
  };
  int status = 0;
  StreamerStatusType streamer_status;
  StreamerStatisticsType streamer_stats;
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
  start_time_error = -7
};

class StreamerStatus {

public:
  StreamerStatus()
      : last_time{std::chrono::system_clock::now()}, partitions{0},
        run_status_(RunStatusError::stopped) {}
  StreamerStatus(const StreamerStatus &other)
      : current{other.current}, last{other.last}, last_time{other.last_time},
        partitions{other.partitions}, run_status_(other.run_status_) {}

  void add_message(const double &bytes) {
    std::unique_lock<std::mutex> lock(m);
    current.bytes += bytes;
    current.bytes2 += bytes * bytes;
    current.messages += 1;
    current.messages2 += 1;
  }
  void error() { current.errors++; }

  StreamerStatusType &&fetch_status() {
    std::unique_lock<std::mutex> lock(m);
    return std::move(last + current);
  }

  StreamerStatisticsType fetch_statistics() {
    StreamerStatisticsType st;
    std::unique_lock<std::mutex> lock(m);

    std::chrono::system_clock::time_point t = std::chrono::system_clock::now();
    auto count = (t - last_time).count();
    st.size_avg = current.bytes / current.messages;
    st.size_std = std::sqrt(
        (current.bytes2 / current.messages - st.size_avg * st.size_avg) /
        current.messages);

    st.freq_avg = current.messages / count;
    st.freq_std = std::sqrt(
        (current.messages / count - st.freq_avg * st.freq_avg) / count);

    last += current;
    last_time = t;
    current.reset();
    return st;
  }
  void run_status(const int8_t value) { run_status_ = value; }

private:
  StreamerStatusType current;
  StreamerStatusType last;
  std::mutex m;
  std::chrono::system_clock::time_point last_time;
  uint32_t partitions;
  int8_t run_status_;
}; // namespace Status

} // namespace Status
} // namespace FileWriter
