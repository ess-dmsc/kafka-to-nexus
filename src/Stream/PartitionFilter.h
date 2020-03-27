// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "KafkaW/PollStatus.h"
#include <chrono>

namespace Stream {
using time_point = std::chrono::system_clock::time_point;
using duration = std::chrono::system_clock::duration;
using std::chrono_literals::operator""s;

/// \brief Implements logic for determining if we should stop consuming data
/// based on message polling status.
///
/// \note This logic is only valid when consuming from a single partition
/// of a topic.
///
/// A diagram showing the logic implemented here can be found in
/// kafka-to-nexus/documentation/PartitionFilter_logic.png
class PartitionFilter {
public:
  PartitionFilter() = default;
  PartitionFilter(time_point StopAtTime, duration StopTimeLeeway,
                  duration ErrorTimeOut);
  /// \brief Update the stop time.
  void setStopTime(time_point Stop) { StopTime = Stop; }
  /// \brief Applies the stop logic to the current poll status.
  /// \param CurrentPollStatus The current (last) poll status.
  /// \return Returns true if consumption from this topic + partition should
  /// be halted.
  bool shouldStopPartition(KafkaW::PollStatus CurrentPollStatus);

  /// \brief Check if we currently have an error state.
  bool hasErrorState() const { return HasError; }

protected:
  bool HasError{false};
  time_point ErrorTime;
  time_point StopTime{time_point::max()};
  duration StopLeeway{10s};
  duration ErrorTimeOut{10s};
};

} // namespace Stream
