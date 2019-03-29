#include "logger.h"
#include <string>
#ifdef HAVE_GRAYLOG_LOGGER
#include <graylog_logger/GraylogInterface.hpp>
#include <graylog_logger/Log.hpp>
#include <spdlog/sinks/graylog_sink.h>
#endif

std::shared_ptr<spdlog::logger> getLogger() {
  return spdlog::get("filewriterlogger");
}

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const uri::URI GraylogURI) {

  std::vector<spdlog::sink_ptr> sinks;
  if (!LogFile.empty()) {
    sinks.push_back(
        std::make_shared<spdlog::sinks::basic_file_sink_mt>(LogFile));
  }
  if (!GraylogURI.getURIString().empty()) {
#ifdef HAVE_GRAYLOG_LOGGER
    sinks.push_back(std::make_shared<spdlog::sinks::graylog_sink_mt>(
        GraylogURI.HostPort.substr(0, GraylogURI.HostPort.find(":")),
        GraylogURI.Port));
#else
    spdlog::log(
        spdlog::level::err,
        "ERROR not compiled with support for graylog_logger. Would have used "
        "{}",
        GraylogURI.HostPort);
#endif
  } else {
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
  }
  auto combined_logger = std::make_shared<spdlog::logger>(
      "filewriterlogger", begin(sinks), end(sinks));
  spdlog::register_logger(combined_logger);
  combined_logger->set_level(LoggingLevel);
  combined_logger->flush_on(spdlog::level::critical);
}
