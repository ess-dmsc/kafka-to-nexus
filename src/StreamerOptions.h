// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "KafkaW/BrokerSettings.h"
#include <chrono>

namespace FileWriter {
using std::chrono_literals::operator""ms;
/// Contains configuration parameters for the Streamer
struct StreamerOptions {
  using clock = std::chrono::system_clock;
  KafkaW::BrokerSettings BrokerSettings;
  clock::time_point StartTime{clock::time_point::min()};
  clock::time_point StopTime{clock::time_point::max()};
  clock::duration BeforeStartTime{1000ms};
  clock::duration AfterStopTime{1000ms};
};

} // namespace FileWriter
