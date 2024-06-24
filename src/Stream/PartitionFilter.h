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

namespace Kafka {
enum class PollStatus;
}

namespace Stream {

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
  enum class PartitionState { DEFAULT, END_OF_PARTITION, ERROR, TIMEOUT };
  PartitionFilter() = default;
  PartitionFilter(time_point StopAtTime, duration StopTimeLeeway,
                  duration TimeLimit);

  /// \brief Update the stop time.
  void setStopTime(time_point Stop) { StopTime = Stop; }

  /// \brief Force shouldStopPartition() to return true on next call.
  void forceStop();

  /// \brief Return true if forceStop() has been called.
  [[nodiscard]] bool hasForceStopBeenRequested() const;

  /// \brief Applies the stop logic to the current poll status.
  /// \param CurrentPollStatus The current (last) poll status.
  /// \return Returns true if consumption from this topic + partition should
  /// be halted.
  [[nodiscard]] bool shouldStopPartition(Kafka::PollStatus CurrentPollStatus);

  [[nodiscard]] PartitionState currentPartitionState() const { return State; }

  /// \brief Check if we currently have an error state.
  [[nodiscard]] bool hasErrorState() const {
    return State == PartitionState::ERROR;
  }

  /// \brief Check if time limit has been exceeded.
  [[nodiscard]] bool hasExceededTimeLimit() const;

  /// \brief Check if topic has timed out.
  [[nodiscard]] bool hasTopicTimedOut() const;

  /// \brief Update status occurence time.
  void updateStatusOccurrenceTime(PartitionState ComparisonState);

  [[nodiscard]] time_point getStatusOccurrenceTime() const {
    return StatusOccurrenceTime;
  }

protected:
  bool ForceStop{false};
  PartitionState State{PartitionState::DEFAULT};
  time_point StatusOccurrenceTime;
  time_point StopTime{time_point::max()};
  duration StopLeeway{10s};
  duration TimeLimit{10s};

private:
  bool at_end_of_partition_{false};
};

} // namespace Stream
