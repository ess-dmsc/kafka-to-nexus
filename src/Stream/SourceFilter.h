// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferMessage.h"
#include "Metrics/Metric.h"
#include "Metrics/Registrar.h"
#include "Stream/MessageWriter.h"
#include <chrono>

namespace Stream {

using time_point = std::chrono::system_clock::time_point;
uint64_t toNanoSec(time_point Time);

class SourceFilter {
public:
  SourceFilter() = default;
  SourceFilter(time_point StartTime, time_point StopTime,
               MessageWriter *Destination, Metrics::Registrar RegisterMetric);
  virtual ~SourceFilter();
  void addDestinationPtr(Message::DestPtrType NewDestination) {
    DestIDs.push_back(NewDestination);
  };

  virtual bool filterMessage(FileWriter::FlatbufferMessage &&InMsg);
  void setStopTime(time_point StopTime);
  time_point getStopTime() { return Stop; }
  virtual bool hasFinished() const;

protected:
  void sendMessage(FileWriter::FlatbufferMessage const &Msg) {
    MessagesTransmitted++;
    for (auto &CDest : DestIDs) {
      Dest->addMessage({CDest, Msg});
    }
  }

  void sendBufferedMessage();
  time_point Start;
  time_point Stop;
  uint64_t CurrentTimeStamp{0};
  MessageWriter *Dest{nullptr};
  bool IsDone{false};
  FileWriter::FlatbufferMessage BufferedMessage;
  std::vector<Message::DestPtrType> DestIDs;
  Metrics::Metric FlatbufferInvalid{"flatbuffer_invalid",
                                    "Flatbuffer failed validation.",
                                    Metrics::Severity::ERROR};
  Metrics::Metric UnorderedTimestamp{
      "unordered_timestamp", "Timestamp of message not in chronological order.",
      Metrics::Severity::ERROR};
  Metrics::Metric RepeatedTimestamp{"repeated_timestamp",
                                    "Got message with repeated timestamp.",
                                    Metrics::Severity::DEBUG};
  Metrics::Metric MessagesReceived{"received",
                                   "Number of messages received/processed.",
                                   Metrics::Severity::DEBUG};
  Metrics::Metric MessagesTransmitted{
      "sent", "Number of messages queued up for writing.",
      Metrics::Severity::DEBUG};
  Metrics::Metric MessagesDiscarded{
      "discarded", "Number of messages discarded for whatever reason.",
      Metrics::Severity::DEBUG};
};

} // namespace Stream
