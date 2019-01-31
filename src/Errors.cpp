#include "Errors.h"

using StreamMasterError = FileWriter::Status::StreamMasterError;

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
  switch (Error) {
  case StreamMasterError::OK:
    return "No Error";
  case StreamMasterError::HAS_FINISHED:
    return "Has Finished";
  case StreamMasterError::RUNNING:
    return "Running";
  case StreamMasterError::NOT_STARTED:
    return "Not Started";
  case StreamMasterError::STREAMER_ERROR:
    return "Streamer Error";
  case StreamMasterError::REPORT_ERROR:
    return "Report Error";
  case StreamMasterError::IS_REMOVABLE:
    return "Is removable";
  case StreamMasterError::EMPTY_STREAMER:
    return "Empty streamer";
  default:
    return "Unknown error code";
  }
}
