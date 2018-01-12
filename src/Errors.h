#pragma once
#include <string>

namespace FileWriter {
namespace Status {

// Error codes for the StreamMaster and Streamers
enum class StreamMasterErrorCode {
  no_error = 1000,
  not_started = 0,
  running = 1,
  has_finished = 2,
  empty_streamer = 3,
  is_removable = 4,
  streamer_error = -1,
  report_failure = -10,
  streammaster_error = -1000
};
enum class StreamerErrorCode {
  no_error = 1000,
  writing = 1,
  has_finished = 0,
  configuration_error = -1,
  consumer_error = -2,
  metadata_error = -3,
  topic_partition_error = -4,
  assign_error = -5,
  topic_error = -6,
  offset_error = -7,
  start_time_error = -8,
  not_initialized = -1000,
};

/// Convert the StreamMasterErrorCode in a string. If the code is not known
/// return "Unknown error code" \param Error the StreamMasterErrorCode to be
/// converted
const std::string
Err2Str(const FileWriter::Status::StreamMasterErrorCode &Error);

/// Convert the StreamerErrorCode in a string. If the code is not known return
/// "Unknown error code" \param Error the StreamerErrorCode to be converted
const std::string Err2Str(const FileWriter::Status::StreamerErrorCode &Error);

} // namespace Status
} // namespace FileWriter
