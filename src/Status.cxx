#include <cmath>

#include <rapidjson/document.h>

#include "Status.hpp"

double average(const double &sum, const double &N) { return sum / N; }

double standard_deviation(const double &sum, const double &sum_squared,
                          const double &N) {
  auto variance = (sum_squared - (sum * sum) / N) / (N - 1);
  if (variance > 0) { // due to numerical instabilities
    return std::sqrt(variance);
  } else {
    return 0.0;
  }
}

const std::pair<double, double>
FileWriter::Status::message_size(const FileWriter::Status::MessageInfo &value) {
  if (value.messages().first == 0) { // nan causes failure in JSON
    return std::pair<double, double>{};
  }
  std::pair<double, double> result(
      average(value.Mbytes().first, value.messages().first),
      standard_deviation(value.Mbytes().first, value.Mbytes().second,
                         value.messages().first));
  return result;
}

const std::pair<double, double> FileWriter::Status::message_frequency(
    const FileWriter::Status::MessageInfo &value,
    const milliseconds &TimeDifference) {
  if (TimeDifference.count() < 1e-10) {
    return std::pair<double, double>({ 0, 0 });
  }
  std::pair<double, double> result(
      1e3 * average(value.messages().first, TimeDifference.count()),
      1e3 * standard_deviation(value.messages().first, value.messages().second,
                               TimeDifference.count()));
  return result;
}

const std::pair<double, double> FileWriter::Status::message_throughput(
    const FileWriter::Status::MessageInfo &value,
    const milliseconds &TimeDifference) {
  if (TimeDifference.count() < 1e-10) {
    return std::pair<double, double>({ 0, 0 });
  }
  std::pair<double, double> result(
      1e3*average(value.Mbytes().first, TimeDifference.count()),
      1e3*standard_deviation(value.Mbytes().first, value.Mbytes().second,
			     TimeDifference.count()));
  return result;
}

// data-race safe += with double
template <class T> void atomic_add(std::atomic<T> &value, const T &other) {
  T current = value.load();
  while (!value.compare_exchange_weak(current, current + other)) {
    current = value.load();
  }
}

const FileWriter::Status::MessageInfo &FileWriter::Status::MessageInfo::
operator=(const FileWriter::Status::MessageInfo &other) {
  Mbytes_.store(other.Mbytes_.load());
  Mbytes_square_.store(other.Mbytes_square_.load());
  messages_.store(other.messages_.load());
  messages_square_.store(other.messages_square_.load());
  errors_.store(other.errors_.load());
  return *this;
}

const FileWriter::Status::MessageInfo &FileWriter::Status::MessageInfo::
operator+=(const FileWriter::Status::MessageInfo &other) {
  atomic_add(Mbytes_, other.Mbytes_.load());
  atomic_add(Mbytes_square_, other.Mbytes_square_.load());
  atomic_add(messages_, other.messages_.load());
  atomic_add(messages_square_, other.messages_square_.load());
  atomic_add(errors_, other.errors_.load());
  return *this;
}

const FileWriter::Status::MessageInfo &
FileWriter::Status::MessageInfo::message(const double &message_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto size = message_size * 1e-6;
  atomic_add(Mbytes_, size);
  atomic_add(Mbytes_square_, size * size);
  atomic_add(messages_, 1.0);
  atomic_add(messages_square_, 1.0);
  return *this;
}

const FileWriter::Status::MessageInfo &
FileWriter::Status::MessageInfo::error() {
  std::lock_guard<std::mutex> lock(mutex_);
  atomic_add(errors_, 1.0);
  return *this;
}

void FileWriter::Status::MessageInfo::reset() {
  Mbytes_ = Mbytes_square_ = 0.0;
  messages_ = messages_square_ = 0.0;
  errors_ = 0.0;
}

std::pair<double, double> FileWriter::Status::MessageInfo::Mbytes() const {
  return std::pair<double, double>{Mbytes_.load(), Mbytes_square_.load()};
}

std::pair<double, double> FileWriter::Status::MessageInfo::messages() const {
  return std::pair<double, double>{messages_.load(), messages_square_.load()};
}

double FileWriter::Status::MessageInfo::errors() const {
  return errors_.load();
}

////////////////////////
// StreamMasterInfo
void FileWriter::Status::StreamMasterInfo::add(
    const std::string &topic, FileWriter::Status::MessageInfo &info) {
  // The lock prevents Streamer::write from update info while
  // collecting these stats, or this function from reset while/after
  // Streamer::write updates. Pairs with MessageInfo::message(error) lock
  if (info_.find(topic) != info_.end()) {
    std::lock_guard<std::mutex> lock(info.mutex());
    info_[topic] = info;
    total_ += info;
    info.reset();
  } else {
    std::lock_guard<std::mutex> lock(info.mutex());
    info_[topic] += info;
    total_ += info;
    info.reset();
  }
}

const milliseconds FileWriter::Status::StreamMasterInfo::time_to_next_message(
    const milliseconds &to_next_message) {
  next_message_relative_eta_ = to_next_message;
  return time_to_next_message();
}
const milliseconds
FileWriter::Status::StreamMasterInfo::time_to_next_message() {
  return next_message_relative_eta_;
}
const milliseconds FileWriter::Status::StreamMasterInfo::runtime() {
  auto result = std::chrono::duration_cast<milliseconds>(
      std::chrono::system_clock::now() - start_time_);
  return result;
}
