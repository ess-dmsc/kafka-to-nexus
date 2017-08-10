#include <map>

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

static const std::map<const int, std::string> stream_master_error_lookup_{
    {FileWriter::Status::StreamMasterErrorCode::not_started, "not_started"},
    {FileWriter::Status::StreamMasterErrorCode::running, "running"},
    {FileWriter::Status::StreamMasterErrorCode::has_finished, "has_finished"},
    {FileWriter::Status::StreamMasterErrorCode::statistics_failure,
     "statistics_failure"},
    {FileWriter::Status::StreamMasterErrorCode::streammaster_error,
     "streammaster_error"}};

static const std::map<const int, std::string> streamer_error_lookup_{
    {FileWriter::Status::StreamerErrorCode::writing, "writing"},
    {FileWriter::Status::StreamerErrorCode::stopped, "stopped"},
    {FileWriter::Status::StreamerErrorCode::configuration_error,
     " configuration_error"},
    {FileWriter::Status::StreamerErrorCode::consumer_error, " consumer_error"},
    {FileWriter::Status::StreamerErrorCode::metadata_error, "metadata_error"},
    {FileWriter::Status::StreamerErrorCode::topic_partition_error,
     "topic_partition_error"},
    {FileWriter::Status::StreamerErrorCode::assign_error, "assign_error"},
    {FileWriter::Status::StreamerErrorCode::topic_error, "topic_error"},
    {FileWriter::Status::StreamerErrorCode::offset_error, "offset_error"},
    {FileWriter::Status::StreamerErrorCode::start_time_error,
     "start_time_error"},
    {FileWriter::Status::StreamerErrorCode::message_error, "message_error"},
      {FileWriter::Status::StreamerErrorCode::write_error, "write_error"},
    {FileWriter::Status::StreamerErrorCode::not_initialized, "not_initialized"}
};

const std::string FileWriter::Status::Err2Str(const FileWriter::StreamMasterError &error) {
  auto it = stream_master_error_lookup_.find(error.value());
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
const std::string FileWriter::Status::Err2Str(const FileWriter::StreamerError &error) {
  auto it = stream_master_error_lookup_.find(error.value());
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
