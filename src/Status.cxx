#include <map>

#include "Status.hpp"

#include <rapidjson/document.h>

FileWriter::Status::StreamerStatusType::StreamerStatusType(
    const FileWriter::Status::StreamerStatusType &other)
    : bytes{other.bytes}, messages{other.messages}, errors{other.errors},
      bytes_squared{other.bytes_squared},
      messages_squared{other.messages_squared} {}

FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator=(FileWriter::Status::StreamerStatusType &&other) noexcept {
  bytes = std::move(other.bytes);
  messages = std::move(other.messages);
  errors = std::move(other.errors);
  bytes_squared = std::move(other.bytes_squared);
  messages_squared = std::move(other.messages_squared);
  return *this;
}
FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator+=(const FileWriter::Status::StreamerStatusType &other) {
  bytes += other.bytes;
  messages += other.messages;
  errors += other.errors;
  bytes_squared += other.bytes_squared;
  messages_squared += other.messages_squared;
  return *this;
}
FileWriter::Status::StreamerStatusType &FileWriter::Status::StreamerStatusType::
operator-=(const FileWriter::Status::StreamerStatusType &other) {
  bytes -= other.bytes;
  messages -= other.messages;
  errors -= other.errors;
  bytes_squared -= other.bytes_squared;
  messages_squared -= other.messages_squared;
  return *this;
}
FileWriter::Status::StreamerStatusType FileWriter::Status::StreamerStatusType::
operator+(const FileWriter::Status::StreamerStatusType &other) {
  FileWriter::Status::StreamerStatusType result(*this);
  return std::move(result += other);
}
void FileWriter::Status::StreamerStatusType::reset() {
  bytes = messages = errors = bytes_squared = messages_squared = 0;
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
  current.bytes_squared += bytes * bytes;
  current.messages += 1;
  current.messages_squared += 1;
}

FileWriter::Status::StreamerStatisticsType
FileWriter::Status::StreamerStatus::fetch_statistics() {
  FileWriter::Status::StreamerStatisticsType st;
  std::unique_lock<std::mutex> lock(m);
  using namespace std::chrono;

  system_clock::time_point t = system_clock::now();
  auto count = duration_cast<seconds>(t - last_time).count();
  if (current.messages > 0) {
    st.average_message_size = current.bytes / current.messages;
    st.standard_deviation_message_size =
        std::sqrt((current.bytes_squared / current.messages -
                   st.average_message_size * st.average_message_size) /
                  current.messages);

    st.average_message_frequency = current.messages / count;
    st.standard_deviation_message_frequency = std::sqrt(
        (current.messages / count -
         st.average_message_frequency * st.average_message_frequency) /
        count);
  } else {
    st.average_message_size = -1.;
    st.standard_deviation_message_size = -1.;
    st.average_message_frequency = -1.;
    st.standard_deviation_message_frequency = -1.;
  }
  last += current;
  last_time = t;
  current.reset();
  return st;
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
    {FileWriter::Status::StreamerErrorCode::not_initialized,
     "not_initialized"}};

const std::string
FileWriter::Status::Err2Str(const FileWriter::StreamMasterError &error) {
  auto it = stream_master_error_lookup_.find(error.value());
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
const std::string
FileWriter::Status::Err2Str(const FileWriter::StreamerError &error) {
  auto it = stream_master_error_lookup_.find(error.value());
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
