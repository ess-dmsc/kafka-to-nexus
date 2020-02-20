// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <spdlog/spdlog.h>
#include <string>

#define UNUSED_ARG(x) (void)x;

namespace uri {
struct URI;
}

using SharedLogger = std::shared_ptr<spdlog::logger>;

SharedLogger getLogger();

void setUpLogging(const spdlog::level::level_enum &LoggingLevel,
                  const std::string &ServiceID, const std::string &LogFile,
                  const uri::URI &GraylogURI);

template<typename... Args>
void LOG_ERROR(spdlog::string_view_t fmt, const Args &... args)
{
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::err, fmt, args...);
}

template<typename... Args>
void LOG_WARN(spdlog::string_view_t fmt, const Args &... args)
{
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::warn, fmt, args...);
}

template<typename... Args>
void LOG_INFO(spdlog::string_view_t fmt, const Args &... args)
{
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::info, fmt, args...);
}

template<typename... Args>
void LOG_DEBUG(spdlog::string_view_t fmt, const Args &... args)
{
  getLogger()->log(spdlog::source_loc{}, spdlog::level::level_enum::debug, fmt, args...);
}
