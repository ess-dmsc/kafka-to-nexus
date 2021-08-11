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

void PartitionFilter::forceStop() { ForceStop = true; }

bool PartitionFilter::shouldStopPartition(Kafka::PollStatus CurrentPollStatus) {
  auto StopFunc = [&](auto ComparisonReason){
    if (Reason != ComparisonReason) {
      Reason = ComparisonReason;
      ErrorTime = std::chrono::system_clock::now();
    } else if (std::chrono::system_clock::now() > ErrorTime + ErrorTimeOut) {
      return true;
    }
    return false;
  };
  if (ForceStop) {
    return true;
  }
  switch (CurrentPollStatus) {
  case Kafka::PollStatus::Message:
    Reason = StopReason::NO_REASON;
    return false;
  case Kafka::PollStatus::EndOfPartition:
    Reason = StopReason::END_OF_PARTITION;
    return std::chrono::system_clock::now() > StopTime + StopLeeway;
  case Kafka::PollStatus::TimedOut:
    return StopFunc(StopReason::TIMEOUT);
  case Kafka::PollStatus::Error:
    return StopFunc(StopReason::ERROR);
  }
  return false;
}

} // namespace Stream
