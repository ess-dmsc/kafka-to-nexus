#pragma once

#include <cstdint>

namespace FileWriter {
  enum class ProcessMessageResult {
    OK,
    ERR,
    ALL_SOURCES_FULL,
    STOP,
  };
} // namespace FileWriter
