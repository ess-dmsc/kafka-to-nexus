#include "Status.hpp"

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
  st.size_avg = current.bytes / current.messages;
  st.size_std = std::sqrt(
      (current.bytes2 / current.messages - st.size_avg * st.size_avg) /
      current.messages);

  st.freq_avg = current.messages / count;
  st.freq_std =
      std::sqrt((current.messages / count - st.freq_avg * st.freq_avg) / count);

  last += current;
  last_time = t;
  current.reset();
  return std::move(st);
}

////////////////////////
// Utility functions
void FileWriter::Status::pprint(
    const FileWriter::Status::StreamerStatusType &x) {
  printf("\tstatus: {\tmessages : %.0lf,\tbytes : %.0lf,\terrors : "
         "%.0lf},\n",
         x.messages, x.bytes, x.errors);
}
void FileWriter::Status::pprint(
    const FileWriter::Status::StreamerStatisticsType &x) {
  printf("\tstatistics: {\tsize average : %.0lf,\tsize std : "
         "%.0lf,\tfrequency average "
         ": %.0lf,\tfrequency std : %.0lf}\n",
         x.size_avg, x.size_std, x.freq_avg, x.freq_std);
}
void FileWriter::Status::pprint(
    const FileWriter::Status::StreamMasterStatus &x) {
  printf("stream_master : %d,\n", x.status);
  for (size_t i = 0; i < x.topic.size(); ++i) {
    printf("%s : {\n", x.topic[i].c_str());
    pprint(x.streamer_status[i]);
    pprint(x.streamer_stats[i]);
    printf("},\n");
  }
};
