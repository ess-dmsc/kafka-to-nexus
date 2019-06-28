#include "logger.h"
#include <string>
#ifdef HAVE_GRAYLOG_LOGGER
#include <graylog_logger/GraylogInterface.hpp>
#include <graylog_logger/Log.hpp>
#include <spdlog/sinks/graylog_sink.h>
#endif

SharedLogger getLogger() { return spdlog::get("filewriterlogger"); }

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string & /*ServiceID*/, const std::string &LogFile,
                  const uri::URI &GraylogURI) {
  std::vector<spdlog::sink_ptr> sinks;
  if (!LogFile.empty()) {
    auto FileSink =
        std::make_shared<spdlog::sinks::basic_file_sink_mt>(LogFile);
    FileSink->set_pattern("[%Y-%m-%d %H:%M:%S.%f] [%l] [processID: %P]: %v");
    sinks.push_back(FileSink);
  }
  if (GraylogURI.getURIString() != "/") {
#ifdef HAVE_GRAYLOG_LOGGER
    auto GraylogSink = std::make_shared<spdlog::sinks::graylog_sink_mt>(
        LoggingLevel,
        GraylogURI.HostPort.substr(0, GraylogURI.HostPort.find(':')),
        GraylogURI.Port);
    GraylogSink->set_pattern("[%l] [processID: %P]: %v");
    sinks.push_back(GraylogSink);
#else
    spdlog::log(
        spdlog::level::err,
        "ERROR not compiled with support for graylog_logger. Would have used{}",
        GraylogURI.HostPort);
#endif
  }
  auto ConsoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  ConsoleSink->set_pattern("[%H:%M:%S.%f] [%l] [processID: %P]: %v");
  sinks.push_back(ConsoleSink);
  auto combined_logger = std::make_shared<spdlog::logger>(
      "filewriterlogger", begin(sinks), end(sinks));
  spdlog::register_logger(combined_logger);
  combined_logger->set_level(LoggingLevel);
  combined_logger->flush_on(spdlog::level::err);
}
