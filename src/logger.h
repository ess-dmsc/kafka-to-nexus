#pragma once

#include "URI.h"
#include <fmt/format.h>
#include <string>
// spdlog.h has to be included before sink headers. Clang-format reverts the
// order so it needs to be turned off.
// clang-format off
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
// clang-format on

using SharedLogger = std::shared_ptr<spdlog::logger>;

SharedLogger getLogger();

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const uri::URI GraylogURI);
