// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <chrono>
#include <ratio>
#include <string>

using time_point = std::chrono::system_clock::time_point;
static_assert(std::ratio_greater_equal<std::milli, time_point::period::type>(),
              "The system clock must have a millisecond resolution or better.");
using duration = std::chrono::system_clock::duration;
using std::chrono_literals::operator""ms;
using std::chrono_literals::operator""s;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"

namespace system_clock {
static auto now() { return std::chrono::system_clock::now(); }
} // namespace system_clock

static auto toNanoSeconds(time_point Timestamp) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Timestamp.time_since_epoch())
      .count();
}

static auto toMicroSeconds(time_point Timestamp) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Timestamp.time_since_epoch())
      .count();
}

static auto toMilliSeconds(time_point Timestamp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             Timestamp.time_since_epoch())
      .count();
}

static auto toMilliSeconds(duration Duration) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Duration)
      .count();
}

static auto toSeconds(time_point Timestamp) {
  return std::chrono::duration_cast<std::chrono::seconds>(
             Timestamp.time_since_epoch())
      .count();
}

std::string toUTCDateTime(time_point TimeStamp);
std::string toLocalDateTime(time_point TimeStamp);

#pragma GCC diagnostic pop
