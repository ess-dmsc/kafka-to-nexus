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

SourceFilter::SourceFilter(time_point start_time, time_point stop_time,
                           bool allow_repeated_timestamps,
                           MessageWriter *writer,
                           std::unique_ptr<Metrics::IRegistrar> registrar)
    : _start_time(start_time), _stop_time(stop_time),
      _allow_repeated_timestamps(allow_repeated_timestamps), _writer(writer),
      _registrar(std::move(registrar)) {
  _registrar->registerMetric(FlatbufferInvalid, {Metrics::LogTo::LOG_MSG});
  _registrar->registerMetric(UnorderedTimestamp, {Metrics::LogTo::LOG_MSG});
  _registrar->registerMetric(MessagesReceived, {Metrics::LogTo::CARBON});
  _registrar->registerMetric(MessagesTransmitted, {Metrics::LogTo::CARBON});
  _registrar->registerMetric(MessagesDiscarded, {Metrics::LogTo::CARBON});
  _registrar->registerMetric(RepeatedTimestamp, {Metrics::LogTo::CARBON});
}

SourceFilter::~SourceFilter() { forward_buffered_message(); }

void SourceFilter::set_stop_time(time_point stop_time) {
  _stop_time = stop_time;
}

bool SourceFilter::has_finished() const { return _is_finished; }

void SourceFilter::forward_buffered_message() {
  if (_buffered_message.isValid()) {
    forward_message(_buffered_message);
    _buffered_message = FileWriter::FlatbufferMessage();
  }
}

time_point to_timepoint(int64_t timestamp) {
  return time_point(std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::nanoseconds(timestamp)));
}

bool SourceFilter::filter_message(
    FileWriter::FlatbufferMessage const &message) {
  if (message.getSourceHash() != _source_hash) {
    // Not intended for this filter
    return false;
  }
  MessagesReceived++;
  if (_is_finished) {
    MessagesDiscarded++;
    return false;
  }
  if (!message.isValid()) {
    MessagesDiscarded++;
    FlatbufferInvalid++;
    return false;
  }

  if (message.getTimestamp() == _last_seen_timestamp) {
    RepeatedTimestamp++;
    if (!_allow_repeated_timestamps) {
      MessagesDiscarded++;
      return false;
    }
  } else if (message.getTimestamp() < _last_seen_timestamp) {
    UnorderedTimestamp++;
  }
  _last_seen_timestamp = message.getTimestamp();

  auto message_time = to_timepoint(message.getTimestamp());
  if (message_time < _start_time) {
    if (_buffered_message.isValid() &&
        message_time < to_timepoint(_buffered_message.getTimestamp())) {
      MessagesDiscarded++;
      return false;
    }
    _buffered_message = message;
    return false;
  }
  if (message_time > _stop_time) {
    _is_finished = true;
  }
  forward_buffered_message();
  forward_message(message);
  return true;
}

void SourceFilter::forward_message(
    FileWriter::FlatbufferMessage const &message) {
  ++MessagesTransmitted;
  for (auto const &writer_module : _destination_writer_modules) {
    _writer->addMessage({writer_module, message});
  }
}

} // namespace Stream
