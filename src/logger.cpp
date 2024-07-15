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
#include <date/date.h>
#include <string>

void setUpLogging(LogSeverity const &logging_level) {
  auto logger = Logger::instance();
  switch (logging_level) {
  case LogSeverity::Critical:
    logger->set_level(spdlog::level::critical);
    break;
  case LogSeverity::Error:
    logger->set_level(spdlog::level::err);
    break;
  case LogSeverity::Warn:
    logger->set_level(spdlog::level::warn);
    break;
  case LogSeverity::Info:
    logger->set_level(spdlog::level::info);
    break;
  case LogSeverity::Debug:
    logger->set_level(spdlog::level::debug);
    break;
  case LogSeverity::Trace:
    logger->set_level(spdlog::level::trace);
    break;
  }
  logger->set_level(spdlog::level::trace);
}
