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
#include "Msg.h"
#include <iostream>

namespace Stream {

PartitionFilter::PartitionFilter(time_point StopAtTime, duration StopTimeLeeway,
                                 duration TimeLimit)
    : StopTime(StopAtTime), StopLeeway(StopTimeLeeway), TimeLimit(TimeLimit) {
  // Deal with potential overflow problem
  if (time_point::max() - StopTime <= StopTimeLeeway) {
    StopTime -= StopTimeLeeway;
  }
}

bool PartitionFilter::hasExceededTimeLimit() const {
  return std::chrono::system_clock::now() > StatusOccurrenceTime + TimeLimit;
}

bool PartitionFilter::hasTopicTimedOut() const {
  return hasExceededTimeLimit() && State == PartitionState::TIMEOUT;
}

void PartitionFilter::updateStatusOccurrenceTime(
    PartitionState ComparisonState) {
  if (State != ComparisonState) {
    State = ComparisonState;
    StatusOccurrenceTime = std::chrono::system_clock::now();
  }
}

void PartitionFilter::forceStop() { ForceStop = true; }

bool PartitionFilter::hasForceStopBeenRequested() const { return ForceStop; }

bool PartitionFilter::shouldStopPartition(Kafka::PollStatus CurrentPollStatus) {
  if (hasForceStopBeenRequested()) {
    return true;
  }
  switch (CurrentPollStatus) {
  case Kafka::PollStatus::Message:
    at_end_of_partition_ = false;
    State = PartitionState::DEFAULT;
    return false;
  case Kafka::PollStatus::EndOfPartition:
    at_end_of_partition_ = true;
    State = PartitionState::END_OF_PARTITION;
    return false;
  case Kafka::PollStatus::TimedOut:
    updateStatusOccurrenceTime(PartitionState::TIMEOUT);
    if (!at_end_of_partition_) {
      return false;
    }
    return std::chrono::system_clock::now() > StopTime + StopLeeway;
  case Kafka::PollStatus::Error:
    updateStatusOccurrenceTime(PartitionState::ERROR);
    return hasExceededTimeLimit();
  }
  return false;
}

} // namespace Stream
