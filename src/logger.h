#pragma once

#include "URI.h"
#include <fmt/format.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>
//#include <spdlog/sinks/graylog_sink.h>
#ifdef _MSC_VER
#define LOG(level, fmt, ...)                                                   \
  { LoggerInstance->log(level, fmt, __VA_ARGS__); }
#else
#define LOG(level, fmt, args...)                                               \
  { LoggerInstance->log(level, fmt, ##args); }
#endif

// These severity level correspond to the RFC3164 (syslog) severity levels
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



std::shared_ptr<spdlog::logger> LoggerInstance;

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const std::string &GraylogURI) {
  spdlog::set_level(LoggingLevel);
  if (not LogFile.empty()) {
    LoggerInstance = spdlog::basic_logger_mt("basic_logger", LogFile);
  }
  if (not GraylogURI.empty()) {
    uri::URI TempURI(GraylogURI);
    // Set up URI interface here
    // auto grayloginterface = spdlog::graylog_sink(TempURI.HostPort,
    // TempURI.Topic);
  }
  LoggerInstance = spdlog::stdout_color_mt("console");
}