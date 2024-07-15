// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "TimeUtility.h"
#include "spdlog/sinks/syslog_sink.h"
#include "spdlog/spdlog.h"
#include <fmt/format.h>
#include <graylog_logger/Log.hpp>
#include <nlohmann/json.hpp>
#include <numeric>
#include <string>
#include <vector>

enum class LogSeverity : int {
  Critical = 0,
  Error = 1,
  Warn = 2,
  Info = 3,
  Debug = 4,
  Trace = 5,
};

template <typename InnerType> struct fmt::formatter<std::vector<InnerType>> {
  static constexpr auto parse(format_parse_context &ctx) {
    const auto begin = ctx.begin();
    const auto end = std::find(begin, ctx.end(), '}');
    return end;
  }

  template <typename FormatContext>
  auto format(const std::vector<InnerType> &Data, FormatContext &ctx) {
    if (Data.empty()) {
      return fmt::format_to(ctx.out(), "[]");
    }
    const int MaxNrOfElements = 10;
    std::string Suffix{};
    auto EndIterator = Data.end();
    if (Data.size() > MaxNrOfElements) {
      EndIterator = Data.begin() + 10;
      Suffix = "...";
    }
    auto ReturnString = std::accumulate(
        std::next(Data.begin()), EndIterator, fmt::format("{}", Data[0]),
        [](std::string const &a, InnerType const &b) {
          return a + fmt::format(", {}", b);
        });
    return fmt::format_to(ctx.out(), "[{}{}]", ReturnString, Suffix);
  }
};

template <> struct fmt::formatter<nlohmann::json> {
  static auto parse(format_parse_context &ctx) {
    const auto begin = ctx.begin();
    const auto end = std::find(begin, ctx.end(), '}');
    return end;
  }

  // clang-format off
  template <typename FormatContext>
  auto format(const nlohmann::json &Data, FormatContext &ctx) { // cppcheck-suppress functionStatic
    auto DataString = Data.dump();
    if (DataString.empty()) {
      return fmt::format_to(ctx.out(), R"("")");
    }
    if (DataString.size() > 30) {
      DataString = DataString.substr(0, 30) + "...";
    }
    return fmt::format_to(ctx.out(), R"("{}")", DataString);
  }
//clang-format on
};

template <> struct fmt::formatter<time_point> {
  static auto parse(format_parse_context &ctx) {
    const auto begin = ctx.begin();
    const auto end = std::find(begin, ctx.end(), '}');
    return end;
  }

  // clang-format off
  template <typename FormatContext>
  auto format(const time_point &TimeStamp, FormatContext &ctx) { // cppcheck-suppress functionStatic
    auto TimeString = toUTCDateTime(TimeStamp);
    return fmt::format_to(ctx.out(), "{}", TimeString);
  }
//clang-format on
};


void setUpLogging(LogSeverity const &logging_level);

struct Logger {
static auto instance() {
  static auto syslog_logger = spdlog::syslog_logger_mt("syslog", "kafka-to-nexus", LOG_PID);
  return syslog_logger;
}

template <typename... Args>
static void Critical(std::string fmt, const Args &... args) {
  instance()->critical(fmt, args...);
}

template <typename... Args>
static void Error(std::string fmt, const Args &... args) {
  instance()->error(fmt, args...);
}

template <typename... Args>
static void Warn(std::string fmt, const Args &... args) {
  instance()->warn(fmt, args...);
}

template <typename... Args>
static void Info(std::string fmt, const Args &... args) {
  instance()->info(fmt, args...);
}

template <typename... Args>
static void Debug(std::string fmt, const Args &... args) {
  instance()->debug(fmt, args...);
}

template <typename... Args>
static void Trace(std::string fmt, const Args &... args) {
  instance()->trace(fmt, args...);
}
};


