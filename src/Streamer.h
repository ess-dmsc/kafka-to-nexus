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

namespace FileWriter {
using ConsumerPtr = std::unique_ptr<KafkaW::ConsumerInterface>;

/// \brief Connect to kafka topics eventually at a given point in time
/// and consume messages.
class Streamer {
  using StreamerStatus = Status::StreamerStatus;

public:
  Streamer() = default;

  /// \brief Create an instance of Streamer.
  ///
  /// \param[in] broker    Broker name or address of one of the brokers in the
  /// partition.
  ///
  /// \param[in] topic_name   Name of the topic to consume.
  /// \param[in] Opts         Opts configuration options for the streamer and
  /// RdKafka.
  ///
  /// \throws     std::runtime_error if failed.
  Streamer(const std::string &Broker, const std::string &TopicName,
           StreamerOptions Opts, ConsumerPtr Consumer);
  Streamer(const Streamer &) = delete;

  ~Streamer() = default;

  /// \brief Polls for message and processes it if there is one
  ///
  /// \param MessageProcessor instance of the policy that describe how to
  /// process the message
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &MessageProcessor);

  /// \brief Processes received message
  ///
  /// \param MessageProcessor instance of the policy that describe how to
  /// process the message
  /// \param KafkaMessage the received message
  ProcessMessageResult processMessage(
      FileWriter::DemuxTopic &MessageProcessor,
      std::unique_ptr<std::pair<KafkaW::PollStatus, Msg>> &KafkaMessage);

  /// \brief Disconnect the kafka consumer and destroy the TopicPartition
  /// vector.
  ///
  /// \remarks Make sure that the Streamer status is
  /// StreamerErrorCode::has_finished.
  StreamerStatus closeStream();

  void setSources(std::unordered_map<std::string, Source> &SourceList);

  /// \brief Returns the status of the Streamer.
  ///
  /// See "Error.h".
  ///
  /// \return The current status.
  StreamerStatus runStatus() const { return RunStatus; }

  /// Return all the information about the messages consumed.
  Status::MessageInfo &messageInfo() { return MessageInfo; }

  /// \brief Return a reference to the Options that has been set for the current
  /// Streamer.
  ///
  /// The method can be used to change the current values.
  StreamerOptions &getOptions() { return Options; }

protected:
  ConsumerPtr Consumer{nullptr};
  KafkaW::BrokerSettings Settings;

  StreamerStatus RunStatus{StreamerStatus::NOT_INITIALIZED};
  Status::MessageInfo MessageInfo;

  std::vector<std::string> Sources;
  StreamerOptions Options;

  std::future<std::pair<Status::StreamerStatus, ConsumerPtr>>
      ConsumerInitialised;

private:
  bool ifConsumerIsReadyThenAssignIt();
  bool stopTimeExceeded(FileWriter::DemuxTopic &MessageProcessor);

  /// Creates StopOffsets vector
  std::vector<std::pair<int64_t, bool>>
  getStopOffsets(std::chrono::milliseconds StopTime,
                 std::string const &TopicName);

  /// Checks whether we've now reached the stop offsets
  bool stopOffsetsReached(int32_t NewMessagePartition,
                          int64_t NewMessageOffset);

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
/// \param[in] TopicName  The topic to consume.
/// \param Logger Pointer to spdlog instance to be used for logging.
///
/// \return If the connection is successful returns ``SEC::writing``. If the
/// consumer can't be created returns ``SEC::configuration_error``, if the topic
/// is not in the partition ``SEC::topic_partition_error``;
std::pair<FileWriter::Status::StreamerStatus, FileWriter::ConsumerPtr>
initTopics(std::string const &TopicName,
           FileWriter::StreamerOptions const &Options,
           SharedLogger const &Logger, ConsumerPtr Consumer);

bool stopTimeElapsed(std::uint64_t MessageTimestamp,
                     std::chrono::milliseconds Stoptime,
                     SharedLogger const &Logger);
} // namespace FileWriter
