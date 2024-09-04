// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "Clock.h"
#include "TimeUtility.h"
#include <memory>

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
  PartitionFilter(
      time_point stop_time, duration stop_time_leeway, duration time_limit,
      std::unique_ptr<Clock> clock = std::make_unique<SystemClock>());

  /// \brief Update the stop time.
  void setStopTime(time_point stop) { _stop_time = stop; }

  /// \brief Applies the stop logic to the current poll status.
  /// \param current_poll_status The current (last) poll status.
  /// \return Returns true if consumption from this topic + partition should
  /// be halted.
  [[nodiscard]] bool shouldStopPartition(Kafka::PollStatus current_poll_status);

  [[nodiscard]] PartitionState currentPartitionState() const { return _state; }

  /// \brief Check if we currently have an error state.
  [[nodiscard]] bool hasErrorState() const {
    return _state == PartitionState::ERROR;
  }

  /// \brief Check if time limit has been exceeded.
  [[nodiscard]] bool hasExceededTimeLimit() const;

  /// \brief Check if topic has timed out.
  [[nodiscard]] bool hasTopicTimedOut() const;

  /// \brief Update status occurence time.
  void updateStatusOccurrenceTime(PartitionState comparison_state);

  [[nodiscard]] time_point getStatusOccurrenceTime() const {
    return _status_occurrence_time;
  }

private:
  PartitionState _state{PartitionState::DEFAULT};
  time_point _status_occurrence_time;
  time_point _stop_time{time_point::max()};
  duration _stop_leeway{10s};
  duration _time_limit{10s};
  std::unique_ptr<Clock> _clock;
  bool _at_end_of_partition{false};
};

} // namespace Stream
