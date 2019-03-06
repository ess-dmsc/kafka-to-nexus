#pragma once

#include "URI.h"
#include <fmt/format.h>
// clang-format off
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/graylog_sink.h>
// clang-format on

#include <string>

#ifdef _MSC_VER

// The levels used in the LOG macro are defined in the spdlog::level namespace
// in spdlog.h
#define LOG(level, fmt, ...)                                                   \
  { spdlog::get("filewriterlogger")->log(level, fmt, __VA_ARGS__); }
#else
#define LOG(level, fmt, args...)                                               \
  { spdlog::get("filewriterlogger")->log(level, fmt, ##args); }
#endif

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const std::string &GraylogURI);