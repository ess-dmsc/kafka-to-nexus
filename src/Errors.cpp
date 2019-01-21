#include "Errors.h"

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

using StreamerStatus = FileWriter::Status::StreamerStatus;

// Utilities

const std::string FileWriter::Status::Err2Str(const StreamerStatus &Error) {
  switch (Error) {
  case StreamerStatus::OK:
    return "No error.";
  case StreamerStatus::WRITING:
    return "Writing";
  case StreamerStatus::HAS_FINISHED:
    return "Has Finished";
  case StreamerStatus::CONFIGURATION_ERROR:
    return "Configuration Error";
  case StreamerStatus::TOPIC_PARTITION_ERROR:
    return "Topic Partition Error";
  case StreamerStatus::NOT_INITIALIZED:
    return "Not Initialized";
  default:
    return "Unknown error code";
  }
}

const std::string FileWriter::Status::Err2Str(const StreamMasterError &Error) {
  switch (Error.Value) {
  case 1000:
    return "No Error";
  case 4:
    return "Stream Can Be Removed";
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
