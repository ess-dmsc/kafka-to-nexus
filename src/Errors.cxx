#include "Errors.h"

//------------------------------------------------------------------------------
// StreamMastererError
using StreamMasterError = FileWriter::Status::StreamMasterError;

StreamMasterError StreamMasterError::OK() {
  StreamMasterError Err;
  Err.Value = 1000;
  return Err;
}

StreamMasterError StreamMasterError::NOT_STARTED() {
  StreamMasterError Err;
  Err.Value = 0;
  return Err;
}

StreamMasterError StreamMasterError::RUNNING() {
  StreamMasterError Err;
  Err.Value = 1;
  return Err;
}

StreamMasterError StreamMasterError::HAS_FINISHED() {
  StreamMasterError Err;
  Err.Value = 2;
  return Err;
}

StreamMasterError StreamMasterError::EMPTY_STREAMER() {
  StreamMasterError Err;
  Err.Value = 3;
  return Err;
}

StreamMasterError StreamMasterError::IS_REMOVABLE() {
  StreamMasterError Err;
  Err.Value = 4;
  return Err;
}

StreamMasterError StreamMasterError::STREAMER_ERROR() {
  StreamMasterError Err;
  Err.Value = -1;
  return Err;
}

StreamMasterError StreamMasterError::REPORT_ERROR() {
  StreamMasterError Err;
  Err.Value = -2;
  return Err;
}

StreamMasterError StreamMasterError::STREAMMASTER_ERROR() {
  StreamMasterError Err;
  Err.Value = -1000;
  return Err;
}

//------------------------------------------------------------------------------
// StreamerError
using StreamerError = FileWriter::Status::StreamerError;

StreamerError StreamerError::OK() {
  StreamerError Err;
  Err.Value = 1000;
  return Err;
}
StreamerError StreamerError::WRITING() {
  StreamerError Err;
  Err.Value = 1;
  return Err;
}
StreamerError StreamerError::HAS_FINISHED() {
  StreamerError Err;
  Err.Value = 0;
  return Err;
}
StreamerError StreamerError::NOT_INITIALIZED() {
  StreamerError Err;
  Err.Value = -1000;
  return Err;
}
StreamerError StreamerError::CONFIGURATION_ERROR() {
  StreamerError Err;
  Err.Value = -1;
  return Err;
}
StreamerError StreamerError::TOPIC_PARTITION_ERROR() {
  StreamerError Err;
  Err.Value = -2;
  return Err;
}
StreamerError StreamerError::UNKNOWN_ERROR() {
  StreamerError Err;
  Err.Value = -1001;
  return Err;
}

//------------------------------------------------------------------------------
// Utilities

const std::string FileWriter::Status::Err2Str(const StreamerError &Error) {
  switch (Error.Value) {
  case 1:
    return "Writing";
  case 0:
    return "Has Finished";
  case -1:
    return "Configuration Error";
  case -2:
    return "Topic Partition Error";
  case -1000:
    return "Not Initialized";
  default:
    return "Unknown error code";
  }
}

const std::string FileWriter::Status::Err2Str(const StreamMasterError &Error) {
  switch (Error.Value) {
  case 1000:
    return "No Error";
  case 3:
    return "Streamers Empty";
  case 2:
    return "Has Finished";
  case 1:
    return "Running";
  case 0:
    return "Not Started";
  case -1:
    return "Streamer Error";
  case -2:
    return "Report Error";
  case -1000:
    return "Generic Error";
  default:
    return "Unknown error code";
  }
}
