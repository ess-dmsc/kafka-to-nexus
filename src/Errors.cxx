#include <map>

#include "Errors.hpp"

static const std::map<const int, std::string> stream_master_error_lookup_{
    {FileWriter::Status::StreamMasterErrorCode::not_started, "not_started"},
    {FileWriter::Status::StreamMasterErrorCode::running, "running"},
    {FileWriter::Status::StreamMasterErrorCode::has_finished, "has_finished"},
    {FileWriter::Status::StreamMasterErrorCode::report_failure,
     "report_failure"},
    {FileWriter::Status::StreamMasterErrorCode::streammaster_error,
     "streammaster_error"}};

static const std::map<const int, std::string> streamer_error_lookup_{
    {FileWriter::Status::StreamerErrorCode::writing, "writing"},
    {FileWriter::Status::StreamerErrorCode::stopped, "stopped"},
    {FileWriter::Status::StreamerErrorCode::configuration_error,
     " configuration_error"},
    {FileWriter::Status::StreamerErrorCode::consumer_error, " consumer_error"},
    {FileWriter::Status::StreamerErrorCode::metadata_error, "metadata_error"},
    {FileWriter::Status::StreamerErrorCode::topic_partition_error,
     "topic_partition_error"},
    {FileWriter::Status::StreamerErrorCode::assign_error, "assign_error"},
    {FileWriter::Status::StreamerErrorCode::topic_error, "topic_error"},
    {FileWriter::Status::StreamerErrorCode::offset_error, "offset_error"},
    {FileWriter::Status::StreamerErrorCode::start_time_error,
     "start_time_error"},
    {FileWriter::Status::StreamerErrorCode::message_error, "message_error"},
    {FileWriter::Status::StreamerErrorCode::write_error, "write_error"},
    {FileWriter::Status::StreamerErrorCode::not_initialized,
     "not_initialized"}};

const std::string
FileWriter::Status::Err2Str(const FileWriter::StreamMasterError &error) {
  auto it = stream_master_error_lookup_.find(error.value());
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
const std::string
FileWriter::Status::Err2Str(const FileWriter::StreamerError &error) {
  auto it = stream_master_error_lookup_.find(error.value());
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
