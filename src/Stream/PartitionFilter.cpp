// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "PartitionFilter.h"

#include "Kafka/PollStatus.h"
#include <memory>
#include <utility>

namespace Stream {

PartitionFilter::PartitionFilter(time_point stop_time,
                                 duration stop_time_leeway, duration time_limit,
                                 std::shared_ptr<Clock> clock)
    : _stop_time(stop_time), _stop_leeway(stop_time_leeway),
      _time_limit(time_limit), clock_(std::move(clock)) {
  // Deal with potential overflow problem
  if (time_point::max() - _stop_time <= stop_time_leeway) {
    _stop_time -= stop_time_leeway;
  }
}

bool PartitionFilter::hasExceededTimeLimit() const {
  return clock_->get_current_time() > _status_occurrence_time + _time_limit;
}

bool PartitionFilter::hasTopicTimedOut() const {
  return hasExceededTimeLimit() && _state == PartitionState::TIMEOUT;
}

void PartitionFilter::updateStatusOccurrenceTime(
    PartitionState comparison_state) {
  if (_state != comparison_state) {
    _state = comparison_state;
    _status_occurrence_time = clock_->get_current_time();
  }
}

void PartitionFilter::forceStop() { _force_stop = true; }

bool PartitionFilter::hasForceStopBeenRequested() const { return _force_stop; }

bool PartitionFilter::shouldStopPartition(
    Kafka::PollStatus current_poll_status) {
  if (hasForceStopBeenRequested()) {
    return true;
  }
  switch (current_poll_status) {
  case Kafka::PollStatus::Message:
    at_end_of_partition_ = false;
    _state = PartitionState::DEFAULT;
    return false;
  case Kafka::PollStatus::EndOfPartition:
    at_end_of_partition_ = true;
    _state = PartitionState::END_OF_PARTITION;
    return false;
  case Kafka::PollStatus::TimedOut:
    updateStatusOccurrenceTime(PartitionState::TIMEOUT);
    if (!at_end_of_partition_) {
      return false;
    }
    return clock_->get_current_time() > _stop_time + _stop_leeway;
  case Kafka::PollStatus::Error:
    updateStatusOccurrenceTime(PartitionState::ERROR);
    return hasExceededTimeLimit();
  }
  return false;
}

} // namespace Stream
