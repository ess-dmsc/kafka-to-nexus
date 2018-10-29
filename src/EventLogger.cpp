#include "EventLogger.h"

std::string FileWriter::convertStatusCodeToString(FileWriter::StatusCode Code) {
  switch (Code) {
  case FileWriter::StatusCode::Start:
    return "START";
  case FileWriter::StatusCode::Close:
    return "CLOSE";
  case FileWriter::StatusCode::Error:
    return "ERROR";
  case FileWriter::StatusCode::Fail:
    return "FAIL";
  default:
    return "UNKNOWN";
  }
};
