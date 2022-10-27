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

namespace Stream {

PartitionFilter::PartitionFilter(time_point StopAtTime, duration StopTimeLeeway,
                                 duration ErrorTimeOut)
    : StopTime(StopAtTime), StopLeeway(StopTimeLeeway),
      ErrorTimeOut(ErrorTimeOut) {
  // Deal with potential overflow problem
  if (time_point::max() - StopTime <= StopTimeLeeway) {
    StopTime -= StopTimeLeeway;
  }
}

bool PartitionFilter::hasExceededErrorTimeOut() {
  return std::chrono::system_clock::now() > ErrorTime + ErrorTimeOut;
}

bool PartitionFilter::hasTopicTimedOut() {
  return hasExceededErrorTimeOut() and State == PartitionState::TIMEOUT;
}

void PartitionFilter::updateErrorTime(PartitionState ComparisonState) {
  if (State != ComparisonState) {
    State = ComparisonState;
    ErrorTime = std::chrono::system_clock::now();
  }
}

void PartitionFilter::forceStop() { ForceStop = true; }

bool PartitionFilter::shouldStopPartition(Kafka::PollStatus CurrentPollStatus) {
  if (ForceStop) {
    return true;
  }
  switch (CurrentPollStatus) {
  case Kafka::PollStatus::Message:
    State = PartitionState::DEFAULT;
    return false;
  case Kafka::PollStatus::EndOfPartition:
    State = PartitionState::END_OF_PARTITION;
    return std::chrono::system_clock::now() > StopTime + StopLeeway;
  case Kafka::PollStatus::TimedOut:
    updateErrorTime(PartitionState::TIMEOUT);
    return false;
  case Kafka::PollStatus::Error:
    updateErrorTime(PartitionState::ERROR);
    return hasExceededErrorTimeOut();
  }
  return false;
}

} // namespace Stream
