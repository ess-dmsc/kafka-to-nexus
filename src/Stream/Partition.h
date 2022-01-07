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
  std::string getMetricsNameString() const {
    return SourceName + "_" + WriterModuleId;
  }
};
using SrcToDst = std::vector<SrcDstKey>;

/// \brief Implements consumption of Kafka messages from partitions and (time
/// based) filtering of those messages.
class Partition {
public:
  Partition() = default;

  Partition(std::unique_ptr<Kafka::ConsumerInterface> Consumer, int Partition,
            std::string TopicName, SrcToDst const &Map, MessageWriter *Writer,
            Metrics::Registrar RegisterMetric, time_point Start,
            time_point Stop, duration StopLeeway, duration KafkaErrorTimeout);
  virtual ~Partition() = default;

  /// \brief Must be called after the constructor.
  /// \note This function exist in order to make unit testing possible.
  void start();

  /// \brief Stop the consumer thread.
  ///
  /// Non blocking. Will tell the consumer thread to stop as soon as possible.
  /// There are no guarantees for when the consumer is actually stopped.
  void stop();

  void setStopTime(time_point Stop);

  virtual bool hasFinished() const;
  auto getPartitionID() const { return PartitionID; }
  auto getTopicName() const { return Topic; }

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

  virtual void pollForMessage();
  virtual void addPollTask();
  virtual bool shouldStopBasedOnPollStatus(Kafka::PollStatus CStatus);
  void forceStop();

  virtual void processMessage(FileWriter::Msg const &Message);
  std::unique_ptr<Kafka::ConsumerInterface> ConsumerPtr;
  int PartitionID{-1};
  std::string Topic{"not_initialized"};
  std::atomic_bool HasFinished{false};
  std::int64_t CurrentOffset{0};
  time_point StopTime;
  duration StopTimeLeeway;
  PartitionFilter StopTester;
  std::vector<std::pair<FileWriter::FlatbufferMessage::SrcHash,
                        std::unique_ptr<SourceFilter>>>
      MsgFilters;
  ThreadedExecutor Executor{false, "partition"}; // Must be last
};

} // namespace Stream
