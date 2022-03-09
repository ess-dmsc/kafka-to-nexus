// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "logger.h"
#include <chrono>
#include <date/date.h>
#include <date/tz.h>

using time_point = std::chrono::system_clock::time_point;

template <class TimeType>
std::string zonedToZuluString(TimeType ZonedTimeStamp) {
  return format("%Y-%m-%dT%H:%M:%SZ", ZonedTimeStamp);
}

template <class TimeType>
std::string zonedToLocalString(TimeType ZonedTimeStamp) {
  return format("%Y-%m-%dT%H:%M:%S%z", ZonedTimeStamp);
}

std::string toUTCDateTime(time_point TimeStamp) {
  const date::time_zone *CurrentTimeZone{date::locate_zone("UTC")};
  auto ZonedTime = date::make_zoned(
      CurrentTimeZone, date::floor<std::chrono::milliseconds>(TimeStamp));
  return zonedToZuluString(ZonedTime);
}

std::string toLocalDateTime(time_point TimeStamp) {
  const date::time_zone *CurrentTimeZone{nullptr};
  try {
    CurrentTimeZone = date::current_zone();
  } catch (const std::runtime_error &e) {
    LOG_WARN("Unable to find local time zone, using UTC.");
    return toUTCDateTime(TimeStamp);
  }
  auto ZonedTime = date::make_zoned(
      CurrentTimeZone, date::floor<std::chrono::milliseconds>(TimeStamp));
  return zonedToLocalString(ZonedTime);
}
