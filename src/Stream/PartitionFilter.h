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
#include "KafkaW/PollStatus.h"

namespace Stream {
using time_point = std::chrono::system_clock::time_point;
using duration = std::chrono::system_clock::duration;

class PartitionFilter {
public:
  PartitionFilter(time_point StopAtTime, duration ErrorTimeOut);
  bool shouldStopPartition(KafkaW::PollStatus LastPollStatus);
protected:
  bool HasError{false};
  time_point ErrorTime;
  time_point StopTime;
  duration MaxErrorTime;
};

} // namespace Stream
