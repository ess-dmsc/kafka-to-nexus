// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "logger.h"
#include "URI.h"
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <string>
#ifdef HAVE_GRAYLOG_LOGGER
#include <graylog_logger/GraylogInterface.hpp>
#include <spdlog/sinks/graylog_sink.h>
#endif

SharedLogger getLogger() { return spdlog::get("filewriterlogger"); }

void setUpLogging(const spdlog::level::level_enum &LoggingLevel, const std::string &LogFile,
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
      "filewriterlogger", cbegin(sinks), cend(sinks));
  spdlog::register_logger(combined_logger);
  combined_logger->set_level(LoggingLevel);
  combined_logger->flush_on(spdlog::level::err);
}
