#pragma once

#include <stdint.h>
#include <string>

namespace FileWriter {

struct MessageTimestamp {
  std::string Sourcename;
  int64_t Timestamp;
  MessageTimestamp(const std::string &sourcename, int64_t Time) : Sourcename(sourcename), Timestamp(Time){};
  MessageTimestamp() = default;
};

} // namespace FileWriter
