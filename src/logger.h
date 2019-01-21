#pragma once

#include "URI.h"
#include <fmt/format.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>
//#include <spdlog/sinks/graylog_sink.h>
#ifdef _MSC_VER

// The levels used in the LOG macro are defined in the spdlog::level namespace in spdlog.h
#define LOG(level, fmt, ...)                                                   \
  { spdlog::get("filewriterlogger")->log(level, fmt, __VA_ARGS__); }
#else
#define LOG(level, fmt, args...)                                               \
  { spdlog::get("filewriterlogger")->log(level, fmt, ##args); }
#endif

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const std::string &GraylogURI) {

//  std::shared_ptr<spdlog::logger> LoggerInstance;

  spdlog::set_level(LoggingLevel);
  if (not LogFile.empty()) {
    spdlog::basic_logger_mt("filewriterlogger", LogFile);
  }
  if (not GraylogURI.empty()) {
    uri::URI TempURI(GraylogURI);
    // Set up URI interface here
    // auto grayloginterface = spdlog::graylog_sink(TempURI.HostPort,
    // TempURI.Topic);
  }
  else {
    spdlog::stdout_color_mt("filewriterlogger");
  }
//  spdlog::register_logger(LoggerInstance);
}