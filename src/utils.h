#pragma once

#include <chrono>

using seconds = std::chrono::seconds;
using milliseconds = std::chrono::milliseconds;
using microseconds = std::chrono::microseconds;
using nanoseconds = std::chrono::nanoseconds;

namespace FileWriter {
namespace utils {

template <typename T, typename PHANTOM> struct StrongType {
public:
  StrongType() {}
  StrongType(T value) : m_value{value} {}
  StrongType(const StrongType &other) : m_value(other.m_value) {}
  inline const T &value() const { return m_value; }
  StrongType &operator=(const StrongType &other) {
    m_value = other.m_value;
    return *this;
  }
  inline bool operator!=(const StrongType &other) {
    return m_value != other.m_value;
  }
  inline bool operator==(const StrongType &other) {
    return m_value == other.m_value;
  }

private:
  T m_value;
};

struct OffsetType {};
struct PartitionType {};
struct Timestamp {};
struct FileWriterErrorType {};
 
 struct StreamerErrorType{};
 struct StreamMasterErrorType{};

} // namespace utils

using RdKafkaOffset = utils::StrongType<int64_t, utils::OffsetType>;
using RdKafkaPartition = utils::StrongType<int32_t, utils::PartitionType>;
using ErrorCode = utils::StrongType<int32_t, utils::FileWriterErrorType>;
using StreamerError = utils::StrongType<int32_t, utils::StreamerErrorType>;
using StreamMasterError = utils::StrongType<int32_t, utils::StreamMasterErrorType>;

const RdKafkaOffset RdKafkaOffsetEnd(-1);
const RdKafkaOffset RdKafkaOffsetBegin(-2);
typedef nanoseconds ESSTimeStamp;

enum StatusCode {
  NO_ERROR = 1000,
  RUNNING = 1,
  STOPPED = 0,
  ERROR = -1,
};
} // namespace FileWriter
