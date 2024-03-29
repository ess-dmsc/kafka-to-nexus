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
                           bool AcceptRepeatedTimestamps,
                           MessageWriter *Destination,
                           std::unique_ptr<Metrics::IRegistrar> RegisterMetric)
    : Start(StartTime), Stop(StopTime),
      WriteRepeatedTimestamps(AcceptRepeatedTimestamps), Dest(Destination),
      Registrar(std::move(RegisterMetric)) {
  Registrar->registerMetric(FlatbufferInvalid, {Metrics::LogTo::LOG_MSG});
  Registrar->registerMetric(UnorderedTimestamp, {Metrics::LogTo::LOG_MSG});
  Registrar->registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  Registrar->registerMetric(MessagesTransmitted, {Metrics::LogTo::CARBON});
  Registrar->registerMetric(MessagesDiscarded, {Metrics::LogTo::CARBON});
  Registrar->registerMetric(RepeatedTimestamp, {Metrics::LogTo::CARBON});
}

SourceFilter::~SourceFilter() { sendBufferedMessage(); }

void SourceFilter::setStopTime(time_point StopTime) { Stop = StopTime; }

bool SourceFilter::hasFinished() const { return IsDone; }

void SourceFilter::sendBufferedMessage() {
  if (BufferedMessage.isValid()) {
    sendMessage(BufferedMessage);
    BufferedMessage = FileWriter::FlatbufferMessage();
  }
}

bool SourceFilter::filterMessage(FileWriter::FlatbufferMessage InMsg) {
  MessagesReceived++;
  if (not InMsg.isValid()) {
    MessagesDiscarded++;
    FlatbufferInvalid++;
    return false;
  }

  if (InMsg.getTimestamp() == CurrentTimeStamp) {
    RepeatedTimestamp++;
    if (not WriteRepeatedTimestamps) {
      MessagesDiscarded++;
      return false;
    }
  } else if (InMsg.getTimestamp() < CurrentTimeStamp) {
    UnorderedTimestamp++;
  }
  CurrentTimeStamp = InMsg.getTimestamp();

  auto Temp = std::chrono::nanoseconds(InMsg.getTimestamp());
  auto TempMsgTime =
      time_point(std::chrono::duration_cast<std::chrono::microseconds>(Temp));
  if (TempMsgTime < Start) {
    if (BufferedMessage.isValid()) {
      MessagesDiscarded++;
    }
    BufferedMessage = InMsg;
    return false;
  }
  if (TempMsgTime > Stop) {
    IsDone = true;
  }
  sendBufferedMessage();
  sendMessage(InMsg);
  return true;
}

} // namespace Stream
