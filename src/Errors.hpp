#pragma once
#include <string>

#include "utils.h"

namespace FileWriter {
namespace Status {

class StreamerStatus;
class StreamMasterStatus;

// Error codes for the StreamMaster and Streamers
enum StreamMasterErrorCode {
  not_started = 0,
  running = 1,
  has_finished = 2,
  empty_streamer = 3,
  streamer_error = -1,
  report_failure = -10,
  streammaster_error = -1000
};
enum StreamerErrorCode {
  no_error = 1001,
  writing = 1,
  stopped = 0,
  configuration_error = -1,
  consumer_error = -2,
  metadata_error = -3,
  topic_partition_error = -4,
  assign_error = -5,
  topic_error = -6,
  offset_error = -7,
  start_time_error = -8,
  message_error = -9,
  write_error = -10,
  not_initialized = -1000,
};

const std::string Err2Str(const FileWriter::StreamMasterError &);
const std::string Err2Str(const FileWriter::StreamerError &);
}
}
