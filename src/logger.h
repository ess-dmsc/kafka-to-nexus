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
#include <iostream>
extern int log_level;

extern std::string g_ServiceID;
#define UNUSED(x) (void)(x)
#define LOG(level, fmt, args...)                                               \
  { temporaryLogger(static_cast<int>(level), fmt, ##args); }

enum class Sev : int {
  Emergency = 0, // Do not use, reserved for system errors
  Alert = 1,     // Do not use, reserved for system errors
  Critical = 2,  // Critical, i.e. wake me up in the middle of the night
  Error = 3,     // Error, I should fix this as soon as possible
  Warning = 4,   // Could be an indication of a problem that needs fixing
  Notice = 5,    // Notice, potentially important events that might need special
                 // treatment
  Info = 6,  // Informational, general run time info for checking the state of
             // the application
  Debug = 7, // Debug, give me a flood of information
};

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const std::string &GraylogURI);

template <typename... TT>
void temporaryLogger(int level, char const *fmt, TT const &... args) {}