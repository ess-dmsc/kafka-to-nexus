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
#include "json.h"
#include <chrono>

namespace FileWriter {

/// Contains configuration parameters for the Streamer
struct StreamerOptions {
  KafkaW::BrokerSettings BrokerSettings;
  std::chrono::milliseconds StartTimestamp{0};
  std::chrono::milliseconds StopTimestamp{0};
  std::chrono::milliseconds BeforeStartTime{1000};
  std::chrono::milliseconds AfterStopTime{1000};
};

} // namespace FileWriter
