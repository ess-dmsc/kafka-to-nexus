#include "EventLogger.h"

std::string FileWriter::convertStatusCodeToString(FileWriter::StatusCode Code) {
  switch (Code) {
  case FileWriter::StatusCode::Start:
    return "START";
    break;
  case FileWriter::StatusCode::Close:
    return "CLOSE";
    break;
  case FileWriter::StatusCode::Error:
    return "ERROR";
    break;
  case FileWriter::StatusCode::Fail:
    return "FAIL";
    break;
  default:
    return "UNKNOWN";
    break;
  }
  return "";
};

void FileWriter::EventLogger::log(FileWriter::StatusCode Code,
                                  const std::string &Message) {
  doLogEvent(Code, Message);
}
