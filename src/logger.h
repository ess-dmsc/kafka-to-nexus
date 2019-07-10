#pragma once

#include "URI.h"
#include <fmt/format.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>

#define UNUSED_ARG(x) (void)x;

using SharedLogger = std::shared_ptr<spdlog::logger>;

SharedLogger getLogger();

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const uri::URI &GraylogURI);
