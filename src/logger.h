// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <nlohmann/json.hpp>
#include <numeric>
#include <spdlog/spdlog.h>
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

  //clang-format off
  template <typename FormatContext>
  auto format(const nlohmann::json &Data,
              FormatContext &ctx) { // cppcheck-suppress functionStatic
    auto DataString = Data.dump();
    if (DataString.empty()) {
      return fmt::format_to(ctx.out(), "\"\"");
    }
    if (DataString.size() > 30) {
      DataString = DataString.substr(0, 30) + "...";
    }
    return fmt::format_to(ctx.out(), "\"{}\"", DataString);
  }
  //clang-format on
};

#define UNUSED_ARG(x) (void)x;

namespace uri {
struct URI;
}

using SharedLogger = std::shared_ptr<spdlog::logger>;

SharedLogger getLogger();

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const uri::URI &GraylogURI);

template <typename... Args>
void LOG_ERROR(spdlog::string_view_t fmt, const Args &... args) {
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::err, fmt,
                   args...);
}

template <typename... Args>
void LOG_WARN(spdlog::string_view_t fmt, const Args &... args) {
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::warn, fmt,
                   args...);
}

template <typename... Args>
void LOG_INFO(spdlog::string_view_t fmt, const Args &... args) {
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::info, fmt,
                   args...);
}

template <typename... Args>
void LOG_DEBUG(spdlog::string_view_t fmt, const Args &... args) {
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::debug, fmt,
                   args...);
}
