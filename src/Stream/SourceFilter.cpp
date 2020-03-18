// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "SourceFilter.h"

namespace Stream {

SourceFilter::SourceFilter(time_point StartTime, time_point StopTime,
                           MessageWriter *Destination,
                           Metrics::Registrar RegisterMetric)
    : Start(StartTime), Stop(StopTime), Dest(Destination) {
  RegisterMetric.registerMetric(FlatbufferInvalid, {Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(UnorderedTimestamp, {Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(MessagesReceived, {Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(MessagesTransmitted, {Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(MessagesDiscarded, {Metrics::LogTo::LOG_MSG});
  RegisterMetric.registerMetric(RepeatedTimestamp, {Metrics::LogTo::LOG_MSG});
}

SourceFilter::~SourceFilter() { sendBufferedMessage(); }

void SourceFilter::setStopTime(time_point StopTime) { Stop = StopTime; }

bool SourceFilter::hasFinished() const { return IsDone; }

uint64_t toNanoSec(time_point Time) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Time.time_since_epoch())
      .count();
}

void SourceFilter::sendBufferedMessage() {
  if (BufferedMessage.isValid()) {
    sendMessage(BufferedMessage);
    BufferedMessage = FileWriter::FlatbufferMessage();
  }
}

bool SourceFilter::filterMessage(FileWriter::FlatbufferMessage &&InMsg) {
  MessagesReceived++;
  if (not InMsg.isValid()) {
    MessagesDiscarded++;
    FlatbufferInvalid++;
    return false;
  }
  if (InMsg.getTimestamp() == CurrentTimeStamp) {
    RepeatedTimestamp++;
  } else if (InMsg.getTimestamp() < CurrentTimeStamp) {
    UnorderedTimestamp++;
  }
  CurrentTimeStamp = InMsg.getTimestamp();

  if (CurrentTimeStamp < toNanoSec(Start)) {
    if (BufferedMessage.isValid()) {
      MessagesDiscarded++;
    }
    BufferedMessage = InMsg;
    return false;
  }
  if (CurrentTimeStamp > toNanoSec(Start) and
      CurrentTimeStamp < toNanoSec(Stop)) {
    sendBufferedMessage();
    sendMessage(InMsg);
    return true;
  }
  sendBufferedMessage();
  sendMessage(InMsg);
  IsDone = true;
  return true;
}

} // namespace Stream
