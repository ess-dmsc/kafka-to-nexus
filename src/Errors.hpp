#pragma once
#include <string>

#include "utils.h"

namespace FileWriter {
namespace Status {

// Error codes for the StreamMaster and Streamers
enum class StreamMasterErrorCode {
  no_error = 1000,
  not_started = 0,
  running = 1,
  has_finished = 2,
  empty_streamer = 3,
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

const std::string Err2Str(const FileWriter::Status::StreamMasterErrorCode &);
const std::string Err2Str(const FileWriter::Status::StreamerErrorCode &);
} // namespace Status
} // namespace FileWriter
