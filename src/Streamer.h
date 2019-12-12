// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file
/// \brief This file contains the declaration of the Streamer class, which
/// consumes kafka logs and calls the write procedure

#pragma once

#include "DemuxTopic.h"
#include "EventLogger.h"
#include "KafkaW/Consumer.h"
#include "Status.h"
#include "StreamerOptions.h"
#include "logger.h"
#include <chrono>
#include <future>
#include <memory>
#include <utility>

namespace FileWriter {
using ConsumerPtr = std::unique_ptr<KafkaW::ConsumerInterface>;
using DemuxPtr = std::shared_ptr<DemuxTopic>;

/// \brief Connect to kafka topics eventually at a given point in time
/// and consume messages.
class Streamer {
  using StreamerStatus = Status::StreamerStatus;

public:
  Streamer() = default;

  /// \brief Create an instance of Streamer.
  ///
  /// \param Broker Broker name or address of one of the brokers in the
  /// partition.
  /// \param TopicName Name of the topic to consume.
  /// \param Opts Opts configuration options for the streamer and
  /// RdKafka.
  /// \param Consumer The Consumer.
  Streamer(const std::string &Broker, const std::string &TopicName,
           StreamerOptions Opts, ConsumerPtr Consumer, DemuxPtr Demuxer);
  Streamer(const Streamer &) = delete;

  ~Streamer() = default;

  /// \brief Polls for message and processes it if there is one
  void pollAndProcess();

  /// \brief Processes received message
  ///
  /// \param MessageProcessor instance of the policy that describe how to
  /// process the message
  /// \param KafkaMessage the received message
  void processMessage(
      std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> &KafkaMessage);

  /// \brief Disconnect the kafka consumer and destroy the TopicPartition
  /// vector.
  ///
  /// \remarks Make sure that the Streamer status is
  /// StreamerErrorCode::has_finished.
  ///
  /// \return The current status.
  StreamerStatus close();

  /// \brief Returns the status of the Streamer.
  ///
  /// See "Error.h".
  ///
  /// \return The current status.
  StreamerStatus runStatus() const { return RunStatus.load(); }

  /// Return all the information about the messages consumed.
  Status::MessageInfo &messageInfo() { return MessageInfo; }

  /// \brief Return a reference to the Options that has been set for the current
  /// Streamer.
  ///
  /// The method can be used to change the current values.
  StreamerOptions &getOptions() { return Options; }

  /// Use to force stream to finish if something has gone wrong
  void setFinished() { RunStatus.store(StreamerStatus::HAS_FINISHED); }

  int getNumberProcessedMessages() { return NumberProcessedMessages.load(); }

protected:
  ConsumerPtr Consumer{nullptr};

  std::atomic<StreamerStatus> RunStatus{StreamerStatus::NOT_INITIALIZED};
  Status::MessageInfo MessageInfo;

  StreamerOptions Options;

  std::future<std::pair<Status::StreamerStatus, ConsumerPtr>>
      ConsumerInitialised;

private:
  std::atomic<int> NumberProcessedMessages{0};
  std::string ConsumerTopicName;
  DemuxPtr MessageProcessor;
  bool ifConsumerIsReadyThenAssignIt();
  bool stopTimeExceeded();

  /// Creates StopOffsets vector
  std::vector<std::pair<int64_t, bool>>
  getStopOffsets(std::chrono::milliseconds StartTime,
                 std::chrono::milliseconds StopTime,
                 std::string const &TopicName);

  /// Checks whether current message means we've now reached the stop offsets
  bool stopOffsetsNowReached(int32_t NewMessagePartition,
                             int64_t NewMessageOffset);

  /// Checks whether we've reached the stop offsets
  bool stopOffsetsReached();

  SharedLogger Logger = getLogger();
  bool CatchingUpToStopOffset = false;

  /// The offset for each partition at which the Streamer should stop consuming
  /// from Kafka and whether it has been reached yet
  /// Only set when the system time reaches the requested stop time
  std::vector<std::pair<int64_t, bool>> StopOffsets;

  /// Check if the consumer has already reached the offset we want to stop at
  void markIfOffsetsAlreadyReached(
      std::vector<std::pair<int64_t, bool>> &OffsetsToStopAt,
      std::string const &TopicName);
};

/// \brief Create a consumer with options specified in the class
/// constructor. Connects to the topic, eventually at the specified timestamp.
///
/// \param TopicName The topic to consume.
/// \param Logger Pointer to spdlog instance to be used for logging.
///
/// \return If the connection is successful returns ``SEC::writing``. If the
/// consumer can't be created returns ``SEC::configuration_error``, if the topic
/// is not in the partition ``SEC::topic_partition_error``;
std::pair<FileWriter::Status::StreamerStatus, FileWriter::ConsumerPtr>
initTopics(std::string const &TopicName,
           FileWriter::StreamerOptions const &Options,
           SharedLogger const &Logger, ConsumerPtr Consumer);
} // namespace FileWriter
