// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <utility>

#include "FlatbufferMessage.h"
#include "Kafka/Consumer.h"
#include "Message.h"
#include "MessageWriter.h"
#include "PartitionFilter.h"
#include "SourceFilter.h"
#include "Stream/MessageWriter.h"
#include "ThreadedExecutor.h"
#include "TimeUtility.h"

namespace Stream {

// Pollution of namespace, fix.
struct SrcDstKey {
  FileWriter::FlatbufferMessage::SrcHash SrcHash;
  FileWriter::FlatbufferMessage::SrcHash WriteHash;
  Message::DestPtrType Destination;
  std::string SourceName;
  std::string FlatbufferId;
  std::string WriterModuleId;
  bool AcceptsRepeatedTimestamps;
  [[nodiscard]] std::string getMetricsNameString() const {
    return SourceName + "_" + WriterModuleId;
  }
};
using SrcToDst = std::vector<SrcDstKey>;

/// \brief Implements consumption of Kafka messages from partitions and (time
/// based) filtering of those messages.
class Partition {
public:
  static std::unique_ptr<Partition>
  create(std::shared_ptr<Kafka::ConsumerInterface> consumer, int partition,
         std::string const &topic_name, SrcToDst const &map,
         MessageWriter *writer, Metrics::IRegistrar *registrar,
         time_point start_time, time_point stop_time, duration stop_leeway,
         duration kafka_error_timeout,
         std::function<bool()> const &streamers_paused_function);

  // Old constructor - to be removed
  Partition(std::shared_ptr<Kafka::ConsumerInterface> consumer, int partition,
            std::string const &topic_name, SrcToDst const &map,
            MessageWriter *writer, Metrics::IRegistrar *registrar,
            time_point start_time, time_point stop_time, duration stop_leeway,
            duration kafka_error_timeout,
            std::function<bool()> const &streamers_paused_function);

  Partition(std::shared_ptr<Kafka::ConsumerInterface> consumer, int partition,
            std::string const &topic_name,
            std::vector<std::unique_ptr<ISourceFilter>> source_filters,
            std::unique_ptr<IPartitionFilter> partition_filter,
            Metrics::IRegistrar *registrar, time_point stop_time,
            duration stop_leeway,
            std::function<bool()> const &streamers_paused_function);
  virtual ~Partition() = default;

  /// \brief Stop the consumer thread.
  ///
  /// Non blocking. Will tell the consumer thread to stop as soon as possible.
  /// There are no guarantees for when the consumer is actually stopped.
  void stop();

  void setStopTime(time_point Stop);

  virtual bool hasFinished() const;
  auto getPartitionID() const { return _partition_id; }
  auto getTopicName() const { return _topic_name; }
  virtual void pollForMessage();

protected:
  Metrics::Metric KafkaTimeouts{"timeouts",
                                "Timeouts when polling for messages."};
  Metrics::Metric KafkaErrors{"kafka_errors",
                              "Errors received when polling for messages.",
                              Metrics::Severity::ERROR};
  Metrics::Metric MessagesReceived{"received",
                                   "Number of messages received from broker."};
  Metrics::Metric MessagesProcessed{
      "processed", "Number of messages queued up for writing."};
  Metrics::Metric BadOffsets{"bad_offsets",
                             "Number of messages received with bad offsets.",
                             Metrics::Severity::ERROR};
  Metrics::Metric EndOfPartition{
      "end_of_partition",
      "Number of times we reached the end of the partition."};

  Metrics::Metric FlatbufferErrors{
      "flatbuffer_errors",
      "Errors when creating flatbuffer message from Kafka message.",
      Metrics::Severity::ERROR};

  Metrics::Metric BufferTooSmallErrors{"flatbuffer_errors.small_buffer",
                                       "Message smaller than 8 bytes errors.",
                                       Metrics::Severity::ERROR};

  Metrics::Metric NotValidFlatbufferErrors{
      "flatbuffer_errors.invalid_flatbuffer",
      "Failed flatbuffer validation errors.", Metrics::Severity::ERROR};

  Metrics::Metric UnknownFlatbufferIdErrors{
      "flatbuffer_errors.unknown_flatbuffer", "Flatbuffer id unknown errors.",
      Metrics::Severity::ERROR};

  Metrics::Metric BadFlatbufferTimestampErrors{
      "flatbuffer_errors.bad_timestamps",
      "Number of messages received with bad timestamps.",
      Metrics::Severity::ERROR};

  virtual bool hasStopBeenRequested() const;
  virtual bool shouldStopBasedOnPollStatus(Kafka::PollStatus CStatus);
  void forceStop();
  /// \brief Checks for occurrence for time out and logs it once for each time
  /// out occurrence.
  void checkAndLogPartitionTimeOut();

  /// \brief Back of stop time if too close to overflowing
  [[nodiscard]] time_point sanitise_stop_time(time_point stop_time);

  /// \brief Sleep.
  /// \note This function exist in order to make unit testing possible.
  virtual void sleep(duration Duration) const;
  virtual void processMessage(FileWriter::Msg const &Message);

  std::shared_ptr<Kafka::ConsumerInterface> _consumer;
  int _partition_id{-1};
  bool _partition_time_out_logged{false};
  std::string _topic_name{"not_initialized"};
  std::atomic_bool _has_finished{false};
  bool _force_stop{false};
  std::int64_t _current_offset{0};
  time_point _stop_time;
  duration _stop_time_leeway{};
  duration _pause_check_interval{200ms};
  std::unique_ptr<IPartitionFilter> _partition_filter;
  std::vector<std::unique_ptr<ISourceFilter>> _source_filters;
  std::function<bool()> _streamers_paused_function;
};

class PartitionThreaded {
public:
  static std::unique_ptr<PartitionThreaded> create(
      std::shared_ptr<Kafka::ConsumerInterface> consumer, int partition_index,
      std::string const &topic_name, SrcToDst const &map, MessageWriter *writer,
      Metrics::IRegistrar *registrar, time_point start_time,
      time_point stop_time, duration stop_leeway, duration kafka_error_timeout,
      std::function<bool()> const &streamers_paused_function) {
    auto partition =
        Partition::create(std::move(consumer), partition_index, topic_name, map,
                          writer, registrar, start_time, stop_time, stop_leeway,
                          kafka_error_timeout, streamers_paused_function);
    return std::make_unique<PartitionThreaded>(std::move(partition));
  }

  explicit PartitionThreaded(std::unique_ptr<Partition> partition)
      : _partition(std::move(partition)),
        _worker_thread(&PartitionThreaded::process, this) {}

  ~PartitionThreaded() {
    _exit.store(true);
    _worker_thread.join();
  }

  void stop() { _immediate_stop_requested.store(true); }

  void set_stop_time(time_point stop_time) {
    _requested_stop_time.store(stop_time);
  }

  bool has_finished() const { return _partition->hasFinished(); }

private:
  void process() {
    while (!_partition->hasFinished() && !_exit) {
      if (_immediate_stop_requested.load()) {
        _partition->stop();
        _immediate_stop_requested.store(false);
      }
      if (_requested_stop_time.load() != time_point::max()) {
        _partition->setStopTime(_requested_stop_time);
        _requested_stop_time.store(time_point::max());
      }
      _partition->pollForMessage();
      std::this_thread::sleep_for(5ms);
    }
  }

  std::atomic<bool> _exit{false};
  std::atomic<time_point> _requested_stop_time{time_point::max()};
  std::unique_ptr<Partition> _partition;
  std::atomic<bool> _immediate_stop_requested{false};
  std::thread _worker_thread;
};

} // namespace Stream
