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
                           bool AcceptRepeatedTimestamps, MessageWriter *writer,
                           std::unique_ptr<Metrics::IRegistrar> RegisterMetric)
    : Start(StartTime), Stop(StopTime),
      WriteRepeatedTimestamps(AcceptRepeatedTimestamps), writer(writer),
      Registrar(std::move(RegisterMetric)) {
  Registrar->registerMetric(FlatbufferInvalid, {Metrics::LogTo::LOG_MSG});
  Registrar->registerMetric(UnorderedTimestamp, {Metrics::LogTo::LOG_MSG});
  Registrar->registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  Registrar->registerMetric(MessagesTransmitted, {Metrics::LogTo::CARBON});
  Registrar->registerMetric(MessagesDiscarded, {Metrics::LogTo::CARBON});
  Registrar->registerMetric(RepeatedTimestamp, {Metrics::LogTo::CARBON});
}

SourceFilter::~SourceFilter() { forward_buffered_message(); }

void SourceFilter::setStopTime(time_point StopTime) { Stop = StopTime; }

bool SourceFilter::hasFinished() const { return IsDone; }

void SourceFilter::forward_buffered_message() {
  if (BufferedMessage.isValid()) {
    forward_message(BufferedMessage);
    BufferedMessage = FileWriter::FlatbufferMessage();
  }
}

time_point to_timepoint(int64_t timestamp) {
  return time_point(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::nanoseconds(timestamp)));
}

bool SourceFilter::filterMessage(FileWriter::FlatbufferMessage const &InMsg) {
  MessagesReceived++;
  if (IsDone) {
    MessagesDiscarded++;
    return false;
  }
  if (!InMsg.isValid()) {
    MessagesDiscarded++;
    FlatbufferInvalid++;
    return false;
  }

  if (InMsg.getTimestamp() == CurrentTimeStamp) {
    RepeatedTimestamp++;
    if (!WriteRepeatedTimestamps) {
      MessagesDiscarded++;
      return false;
    }
  } else if (InMsg.getTimestamp() < CurrentTimeStamp) {
    UnorderedTimestamp++;
  }
  CurrentTimeStamp = InMsg.getTimestamp();

  auto message_time = to_timepoint(InMsg.getTimestamp());
  if (message_time < Start) {
    if (BufferedMessage.isValid() &&
        message_time < to_timepoint(BufferedMessage.getTimestamp())) {
      MessagesDiscarded++;
      return false;
    }
    BufferedMessage = InMsg;
    return false;
  }
  if (message_time > Stop) {
    IsDone = true;
  }
  forward_buffered_message();
  forward_message(InMsg);
  return true;
}

void SourceFilter::forward_message(
    FileWriter::FlatbufferMessage const &message) {
  ++MessagesTransmitted;
  for (auto const &writer_module : destination_writer_modules) {
    writer->addMessage({writer_module, message});
  }
}

} // namespace Stream
