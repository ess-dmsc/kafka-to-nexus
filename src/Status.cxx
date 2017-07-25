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
FileWriter::Status::StreamerStatusType& FileWriter::Status::StreamerStatusType::
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
FileWriter::Status::StreamerStatusType
FileWriter::Status::StreamerStatusType::
operator+(const FileWriter::Status::StreamerStatusType &other) {
  FileWriter::Status::StreamerStatusType result(*this);
  return std::move(result += other);
}
void FileWriter::Status::StreamerStatusType::reset() {
  bytes = messages = errors = bytes2 = messages2 = 0;
  return;
}
