#include "Status.hpp"
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

FileWriter::Status::StreamerStatusType::StreamerStatusType()
    : bytes{0}, messages{0}, errors{0}, bytes2{0}, messages2{0} {}

FileWriter::Status::StreamerStatusType::StreamerStatusType(
    const FileWriter::Status::StreamerStatusType &other)
    : bytes{other.bytes}, messages{other.messages}, errors{other.errors},
      bytes2{other.bytes2}, messages2{other.messages2} {}

FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator=(const FileWriter::Status::StreamerStatusType &other) {
  bytes = other.bytes;
  messages = other.messages;
  errors = other.errors;
  bytes2 = other.bytes2;
  messages2 = other.messages2;
  return *this;
}
FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator=(FileWriter::Status::StreamerStatusType &&other) {
  bytes = std::move(other.bytes);
  messages = std::move(other.messages);
  errors = std::move(other.errors);
  bytes2 = std::move(other.bytes2);
  messages2 = std::move(other.messages2);
  return *this;
}
FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator+=(const FileWriter::Status::StreamerStatusType &other) {
  bytes += other.bytes;
  messages += other.messages;
  errors += other.errors;
  bytes2 += other.bytes2;
  messages2 += other.messages2;
  return *this;
}
FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator-=(const FileWriter::Status::StreamerStatusType &other) {
  bytes -= other.bytes;
  messages -= other.messages;
  errors -= other.errors;
  bytes2 -= other.bytes2;
  messages2 -= other.messages2;
  return *this;
}
FileWriter::Status::StreamerStatusType FileWriter::Status::StreamerStatusType::
operator+(const FileWriter::Status::StreamerStatusType &other) {
  FileWriter::Status::StreamerStatusType result(*this);
  return std::move(result += other);
}
void FileWriter::Status::StreamerStatusType::reset() {
  bytes = messages = errors = bytes2 = messages2 = 0;
  return;
}

////////////////////////////////
// StreamerStatus implementation
FileWriter::Status::StreamerStatusType
FileWriter::Status::StreamerStatus::fetch_status() {
  std::unique_lock<std::mutex> lock(m);
  return std::move(last + current);
}

void FileWriter::Status::StreamerStatus::add_message(const double &bytes) {
  std::unique_lock<std::mutex> lock(m);
  current.bytes += bytes;
  current.bytes2 += bytes * bytes;
  current.messages += 1;
  current.messages2 += 1;
}

FileWriter::Status::StreamerStatisticsType
FileWriter::Status::StreamerStatus::fetch_statistics() {
  FileWriter::Status::StreamerStatisticsType st;
  std::unique_lock<std::mutex> lock(m);

  std::chrono::system_clock::time_point t = std::chrono::system_clock::now();
  auto count = (t - last_time).count();
  if (current.messages > 0) {
    st.size_avg = current.bytes / current.messages;
    st.size_std = std::sqrt(
        (current.bytes2 / current.messages - st.size_avg * st.size_avg) /
        current.messages);

    st.freq_avg = current.messages / count;
    st.freq_std = std::sqrt(
        (current.messages / count - st.freq_avg * st.freq_avg) / count);
  } else {
    st.size_avg = -1.;
    st.size_std = -1.;
    st.freq_avg = -1.;
    st.freq_std = -1.;
  }
  last += current;
  last_time = t;
  current.reset();
  return std::move(st);
}
