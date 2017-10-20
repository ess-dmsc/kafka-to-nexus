#include <map>

#include "Errors.hpp"

using SMEC = FileWriter::Status::StreamMasterErrorCode;
using SEC = FileWriter::Status::StreamerErrorCode;

static const std::map<const SMEC, std::string> stream_master_error_lookup_{
    {SMEC::not_started, "not_started"},
    {SMEC::running, "running"},
    {SMEC::has_finished, "has_finished"},
    {SMEC::report_failure, "report_failure"},
    {SMEC::streammaster_error, "streammaster_error"}};

static const std::map<const int, std::string> streamer_error_lookup_{
    {SEC::writing, "writing"},
    {SEC::stopped, "stopped"},
    {SEC::configuration_error,
     " configuration_error"},
    {SEC::consumer_error, " consumer_error"},
    {SEC::metadata_error, "metadata_error"},
    {SEC::topic_partition_error,
     "topic_partition_error"},
    {SEC::assign_error, "assign_error"},
    {SEC::topic_error, "topic_error"},
    {SEC::offset_error, "offset_error"},
    {SEC::start_time_error,
     "start_time_error"},
    {SEC::message_error, "message_error"},
    {SEC::write_error, "write_error"},
    {SEC::not_initialized,
     "not_initialized"}};

const std::string
FileWriter::Status::Err2Str(const SMEC &error) {
  auto it = stream_master_error_lookup_.find(error);
  if (it != stream_master_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
const std::string
FileWriter::Status::Err2Str(const FileWriter::StreamerError &error) {
  auto it = streamer_error_lookup_.find(error.value());
  if (it != streamer_error_lookup_.end()) {
    return it->second;
  }
  return "Unknown error code";
};
