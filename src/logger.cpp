#include "logger.h"

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
  } else {
    spdlog::stdout_color_mt("filewriterlogger");
  }
  //  spdlog::register_logger(LoggerInstance);
}