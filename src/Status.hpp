#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

#include <chrono>
#include <map>

#include "Errors.hpp"

namespace FileWriter {
namespace Status {

class StdIOWriter;

class MessageInfo {
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
  std::mutex& mutex() { return mutex_; }

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

  void add(const std::string &topic, MessageInfo &info);

  value_type &info() { return info_; }
  MessageInfo &total() { return total_; }
  SMEC &status(const SMEC &other) { status_ = other; return status_; }
  SMEC &status() { return status_; }

  const double time(const std::chrono::milliseconds& elapsed_time);
  const double time();

private:
  MessageInfo total_;
  value_type info_;
  std::chrono::milliseconds time_;
  SMEC status_{SMEC::not_started};
};

const std::pair<double, double> message_size(const MessageInfo &);
const std::pair<double, double> message_frequency(const MessageInfo &,
                                                  const double);
const std::pair<double, double> message_throughput(const MessageInfo &,
                                                   const double);


} // namespace Status
} // namespace FileWriter
