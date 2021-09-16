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
#include <graylog_logger/ConsoleInterface.hpp>
#include <graylog_logger/FileInterface.hpp>
#include <graylog_logger/GraylogInterface.hpp>
#include <string>

std::string consoleFormatter(Log::LogMessage const &Msg) {
  std::array<std::string, 8> const sevToStr = {{"EMERGENCY", "ALERT",
                                                "CRITICAL", "ERROR", "WARNING",
                                                "Notice", "Info", "Debug"}};
  return fmt::format("{} [{}] {}", date::format("[%H:%M:%S] ", Msg.Timestamp),
                     sevToStr[int(Msg.SeverityLevel)], Msg.MessageString);
}

void setUpLogging(Log::Severity const &LoggingLevel,
                  const std::string &LogFileName, const uri::URI &GraylogURI) {
  Log::SetMinimumSeverity(LoggingLevel);
  auto Handlers = Log::GetHandlers();
  for (auto &Handler : Handlers) {
    if (dynamic_cast<Log::ConsoleInterface *>(Handler.get()) != nullptr) {
      dynamic_cast<Log::ConsoleInterface *>(Handler.get())
          ->setMessageStringCreatorFunction(consoleFormatter);
    }
  }
  if (!LogFileName.empty()) {
    Log::AddLogHandler(std::make_shared<Log::FileInterface>(LogFileName));
  }
  if (GraylogURI.getURIString() != "/") {
    Log::AddLogHandler(std::make_shared<Log::GraylogInterface>(
        GraylogURI.Host, GraylogURI.Port));
  }
}
