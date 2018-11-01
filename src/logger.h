#pragma once

#include <fmt/format.h>
#include <graylog_logger/Log.hpp>
#include <string>

// These severity level correspond to the RFC3164 (syslog) severity levels
using Sev = Log::Severity;

inline Sev ForceSev(Sev Level) { // Force the use of the correct type
  return Level;
}

template <char Slash> inline std::string reduceFilePath(std::string Path) {
  auto PrefixLoc = Path.rfind(std::string("src") + Slash);
  if (PrefixLoc == std::string::npos) {
    return Path;
  }
  return Path.substr(PrefixLoc + 4, std::string::npos);
}

#ifdef _MSC_VER

#define LOG(Severity, Format, ...)                                             \
  Log::Msg(ForceSev(Severity), fmt::format(Format, __VA_ARGS__),               \
           {{"file", reduceFilePath<'\\'>(__FILE__)},                          \
            {"line", std::int64_t(__LINE__)},                                  \
            {"function", std::string(__FUNCSIG__)}})

#else

#define LOG(Severity, Format, ...)                                             \
  Log::Msg(ForceSev(Severity), fmt::format(Format, ##__VA_ARGS__),             \
           {{"file", reduceFilePath<'/'>(__FILE__)},                           \
            {"line", std::int64_t(__LINE__)},                                  \
            {"function", std::string(__PRETTY_FUNCTION__)}})

#endif

void setupLogging(Sev LoggingLevel, std::string ServiceID, std::string LogFile,
                  std::string GraylogAddress, std::string KafkaGraylogUri);
