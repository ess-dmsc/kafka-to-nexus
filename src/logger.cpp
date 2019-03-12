#include "logger.h"

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const std::string &GraylogURI) {

  std::vector<spdlog::sink_ptr> sinks;
  if (!LogFile.empty()) {
    sinks.push_back(
        std::make_shared<spdlog::sinks::basic_file_sink_mt>(LogFile));
  }
  if (!GraylogURI.empty()) {
    uri::URI TempURI(GraylogURI);
    sinks.push_back(std::make_shared<spdlog::sinks::graylog_sink_mt>(
        TempURI.HostPort.substr(0, TempURI.HostPort.find(":")), TempURI.Port));
  } else {
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
  }
  auto combined_logger = std::make_shared<spdlog::logger>(
      "filewriterlogger", begin(sinks), end(sinks));
  spdlog::register_logger(combined_logger);
  combined_logger->set_level(LoggingLevel);
}