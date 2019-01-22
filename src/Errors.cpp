#include "Errors.h"

using StreamMasterError = FileWriter::Status::StreamMasterError;

StreamMasterError StreamMasterError::OK() { return {1000}; }

StreamMasterError StreamMasterError::NOT_STARTED() { return {0}; }

StreamMasterError StreamMasterError::RUNNING() { return {1}; }

StreamMasterError StreamMasterError::HAS_FINISHED() { return {2}; }

StreamMasterError StreamMasterError::EMPTY_STREAMER() { return {3}; }

StreamMasterError StreamMasterError::IS_REMOVABLE() { return {4}; }

StreamMasterError StreamMasterError::STREAMER_ERROR() { return {-1}; }

StreamMasterError StreamMasterError::REPORT_ERROR() { return {-2}; }

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
