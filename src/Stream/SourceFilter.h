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
#include "TimeUtility.h"

namespace Stream {

/// \brief Pass messages to the writer thread based on timestamp of message
/// and if there are any destinations in the file for the data.
/// SourceFilter buffers a message such that, when used in conjunction with the
/// periodic-update feature of the Forwarder, the writer module should always be
/// able to record at least the data from a single message.
class SourceFilter {
public:
  SourceFilter() = default;
  SourceFilter(time_point StartTime, time_point StopTime,
               bool AcceptRepeatedTimestamps, MessageWriter *writer,
               std::unique_ptr<Metrics::IRegistrar> RegisterMetric);
  virtual ~SourceFilter();
  void add_writer_module_for_message(Message::DestPtrType writer_module) {
    destination_writer_modules.push_back(writer_module);
  };

  virtual bool filterMessage(FileWriter::FlatbufferMessage InMsg);
  void setStopTime(time_point StopTime);
  time_point getStopTime() const { return Stop; }
  virtual bool hasFinished() const;

protected:
  void sendMessage(FileWriter::FlatbufferMessage const &message);
  void sendBufferedMessage();
  time_point Start;
  time_point Stop;
  bool WriteRepeatedTimestamps;
  int64_t CurrentTimeStamp{0};
  MessageWriter *writer{nullptr};
  bool IsDone{false};
  FileWriter::FlatbufferMessage BufferedMessage;
  std::vector<Message::DestPtrType> destination_writer_modules;
  std::unique_ptr<Metrics::IRegistrar> Registrar;
  Metrics::Metric FlatbufferInvalid{"flatbuffer_invalid",
                                    "Flatbuffer failed validation.",
                                    Metrics::Severity::ERROR};
  Metrics::Metric UnorderedTimestamp{
      "unordered_timestamp", "Timestamp of message not in chronological order.",
      Metrics::Severity::WARNING};
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
