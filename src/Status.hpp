#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include <map>

#include "Errors.hpp"

namespace FileWriter {
namespace Status {

class MessageInfo {
  using SMC = StreamerErrorCode;

public:
  using value_type = std::pair<double, double>;
  const MessageInfo &operator=(const MessageInfo &other);
  const MessageInfo &operator+=(const MessageInfo &other);

  const MessageInfo &message(const double &message_size);
  const MessageInfo &error();

  void reset();

  value_type Mbytes() const;
  value_type messages() const;
  double errors() const;
  std::mutex &mutex() { return mutex_; }

private:
  std::atomic<double> messages_{0};
  std::atomic<double> messages_square_{0};
  std::atomic<double> Mbytes_{0};
  std::atomic<double> Mbytes_square_{0};
  std::atomic<double> errors_{0};
  std::mutex mutex_;
};

class StreamMasterInfo {
  using SMEC = StreamMasterErrorCode;

public:
  using value_type = std::map<std::string, MessageInfo>;
  StreamMasterInfo() : start_time_{std::chrono::system_clock::now()} {}

  void add(const std::string &topic, MessageInfo &info);

  value_type &info() { return info_; }
  MessageInfo &total() { return total_; }
  SMEC &status(const SMEC &other) {
    status_ = other;
    return status_;
  }
  SMEC &status() { return status_; }

  const milliseconds time_to_next_message(const milliseconds &to_next_message);
  const milliseconds time_to_next_message();
  const milliseconds runtime();

private:
  MessageInfo total_;
  value_type info_;
  std::chrono::system_clock::time_point start_time_;
  milliseconds next_message_relative_eta_;
  SMEC status_{SMEC::not_started};
};

const std::pair<double, double> message_size(const MessageInfo &Information);
const std::pair<double, double>
message_frequency(const MessageInfo &Information, const milliseconds &Duration);
const std::pair<double, double>
message_throughput(const MessageInfo &Information,
                   const milliseconds &Duration);

} // namespace Status
} // namespace FileWriter
