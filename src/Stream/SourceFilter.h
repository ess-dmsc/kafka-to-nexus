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

/// \brief Pass messages to the _writer thread based on timestamp of message
/// and if there are any destinations in the file for the data.
/// SourceFilter buffers a message such that, when used in conjunction with the
/// periodic-update feature of the Forwarder, the _writer module should always
/// be able to record at least the data from a single message.
class SourceFilter {
public:
  SourceFilter() = default;
  SourceFilter(time_point start_time, time_point stop_time,
               bool accept_repeated_timestamps, MessageWriter *writer,
               std::unique_ptr<Metrics::IRegistrar> registrar);
  virtual ~SourceFilter();
  void add_writer_module_for_message(Message::DestPtrType writer_module) {
    _destination_writer_modules.push_back(writer_module);
  };

  virtual bool filter_message(FileWriter::FlatbufferMessage const &message);
  void set_stop_time(time_point stop_time);
  time_point get_stop_time() const { return _stop_time; }
  virtual bool has_finished() const;
  void set_source_hash(FileWriter::FlatbufferMessage::SrcHash source_hash) {
    if (_source_hash != 0) {
      Logger::Warn("Source hash should only be set once");
    }
    _source_hash = source_hash;
  }

private:
  void forward_message(FileWriter::FlatbufferMessage const &message);
  void forward_buffered_message();
  time_point _start_time;
  time_point _stop_time;
  bool _allow_repeated_timestamps{false};
  int64_t _last_seen_timestamp{0};
  MessageWriter *_writer{nullptr};
  bool _is_finished{false};
  FileWriter::FlatbufferMessage _buffered_message;
  std::vector<Message::DestPtrType> _destination_writer_modules;
  std::unique_ptr<Metrics::IRegistrar> _registrar;
  FileWriter::FlatbufferMessage::SrcHash _source_hash{0};
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
