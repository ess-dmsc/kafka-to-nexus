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

double average(const double& sum, const double& N) {
  return sum/N;
}

double standard_deviation(const double& sum, const double& sum_squared, const double& N) {
  return std::sqrt( (N*sum_squared - sum*sum)/(N*(N-1)) );
}

FileWriter::Status::StreamerStatusType
FileWriter::Status::StreamerStatus::fetch_status() {
  std::unique_lock<std::mutex> lock(m);
  return std::move(last + current);
}

void FileWriter::Status::StreamerStatus::add_message(const double &bytes) {
  std::unique_lock<std::mutex> lock(m);
  current.bytes += bytes;
  current.bytes_squared += bytes * bytes;
  current.messages++;
  current.messages_squared++;
}

FileWriter::Status::StreamerStatisticsType
FileWriter::Status::StreamerStatus::fetch_statistics() {
  FileWriter::Status::StreamerStatisticsType st;
  std::unique_lock<std::mutex> lock(m);
  using namespace std::chrono;

  system_clock::time_point t = system_clock::now();
  if (current.messages > 0) {
    st.average_message_size = average(current.bytes,current.messages);
    st.standard_deviation_message_size = standard_deviation(current.bytes, current.bytes_squared, current.messages);

    double elapsed_time = duration_cast<seconds>(t - last_time).count();
    st.average_message_frequency = average(current.messages, elapsed_time);
    st.standard_deviation_message_frequency = standard_deviation(current.messages, current.messages_squared, elapsed_time);
  } else {
    st.average_message_size = 0.;
    st.standard_deviation_message_size = 0.;
    st.average_message_frequency = 0.;
    st.standard_deviation_message_frequency = 0.;
  }
  last += current;
  last_time = t;
  current.reset();
  return st;
}

//////////////////////
// StreamMasterStatus

FileWriter::Status::StreamMasterStatus& 
FileWriter::Status::StreamMasterStatus::push(const std::string &topic_name,
					     const FileWriter::Status::StreamerStatusType &status,
					     const FileWriter::Status::StreamerStatisticsType &stats) {
  topic.push_back(topic_name);
  streamer_status.push_back(status);
  streamer_stats.push_back(stats);
  return *this;
}
