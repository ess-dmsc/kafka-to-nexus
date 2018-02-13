#include <map>

#include "Errors.h"

using SMEC = FileWriter::Status::StreamMasterErrorCode;
using SEC = FileWriter::Status::StreamerErrorCode;

static const std::map<const SMEC, std::string> StreamMasterErrorLookup{
    {SMEC::no_error, "no_error"},
    {SMEC::not_started, "not_started"},
    {SMEC::running, "running"},
    {SMEC::has_finished, "has_finished"},
    {SMEC::is_removable, "is_removable"},
    {SMEC::report_failure, "report_failure"},
    {SMEC::streammaster_error, "streammaster_error"}};

static const std::map<const SEC, std::string> StreamerErrorLookup{
    {SEC::writing, "writing"},
    {SEC::has_finished, "has_finished"},
    {SEC::configuration_error, " configuration_error"},
    {SEC::consumer_error, " consumer_error"},
    {SEC::metadata_error, "metadata_error"},
    {SEC::topic_partition_error, "topic_partition_error"},
    {SEC::assign_error, "assign_error"},
    {SEC::topic_error, "topic_error"},
    {SEC::offset_error, "offset_error"},
    {SEC::start_time_error, "start_time_error"},
    {SEC::not_initialized, "not_initialized"}};

const std::string FileWriter::Status::Err2Str(const SMEC &error) {
  auto it = StreamMasterErrorLookup.find(error);
  if (it != StreamMasterErrorLookup.end()) {
    return it->second;
  }
  return "Unknown error code";
}
const std::string FileWriter::Status::Err2Str(const SEC &error) {
  auto it = StreamerErrorLookup.find(error);
  if (it != StreamerErrorLookup.end()) {
    return it->second;
  }
  return "Unknown error code";
}
