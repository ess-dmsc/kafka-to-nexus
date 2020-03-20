// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "PartitionFilter.h"

namespace Stream {

PartitionFilter::PartitionFilter(Stream::time_point StopAtTime,
                                 duration StopTimeLeeway,
                                 Stream::duration ErrorTimeOut)
    : StopTime(StopAtTime), StopLeeway(StopTimeLeeway),
      ErrorTimeOut(ErrorTimeOut) {
  if (time_point::max() - StopTime <=
      StopTimeLeeway) { // Deal with potential overflow problem
    StopTime -= StopTimeLeeway;
  }
}

bool PartitionFilter::shouldStopPartition(
    KafkaW::PollStatus CurrentPollStatus) {
  switch (CurrentPollStatus) {
  case KafkaW::PollStatus::Empty:
  case KafkaW::PollStatus::Message:
  case KafkaW::PollStatus::TimedOut:
    HasError = false;
    return false;
    break;
  case KafkaW::PollStatus::EndOfPartition:
    HasError = false;
    return std::chrono::system_clock::now() > StopTime + StopLeeway;
  case KafkaW::PollStatus::Error:
    if (not HasError) {
      HasError = true;
      ErrorTime = std::chrono::system_clock::now();
    } else if (std::chrono::system_clock::now() > ErrorTime + ErrorTimeOut) {
      return true;
    }
    break;
  }
  return false;
}

} // namespace Stream
