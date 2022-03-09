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
#include <fmt/format.h>
#include <graylog_logger/Log.hpp>
#include <nlohmann/json.hpp>
#include <numeric>
#include <string>
#include <vector>

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

#define UNUSED_ARG(x) (void)x;

namespace uri {
struct URI;
}

void setUpLogging(const Log::Severity &LoggingLevel,
                  const std::string &LogFileName, const uri::URI &GraylogURI);

template <typename... Args>
void LOG_ERROR(std::string fmt, const Args &... args) {
  Log::FmtMsg(Log::Severity::Error, fmt, args...);
}

template <typename... Args>
void LOG_WARN(std::string fmt, const Args &... args) {
  Log::FmtMsg(Log::Severity::Warning, fmt, args...);
}

template <typename... Args>
void LOG_INFO(std::string fmt, const Args &... args) {
  Log::FmtMsg(Log::Severity::Info, fmt, args...);
}

template <typename... Args>
void LOG_DEBUG(std::string fmt, const Args &... args) {
  Log::FmtMsg(Log::Severity::Debug, fmt, args...);
}

template <typename... Args>
void LOG_TRACE(std::string fmt, const Args &... args) {
  Log::FmtMsg(Log::Severity::Debug, fmt, args...);
// Fix this, what do we do about log levels?
}
