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
  /// \param Broker Broker name or address of one of the brokers in the
  /// partition.
  /// \param TopicName Name of the topic to consume.
  /// \param Opts Opts configuration options for the streamer and
  /// RdKafka.
  /// \param Consumer The Consumer.
  Streamer(const std::string &Broker, const std::string &TopicName,
           StreamerOptions Opts, ConsumerPtr Consumer);
  Streamer(const Streamer &) = delete;

  ~Streamer() = default;

  /// \brief Method that process a message.
  ///
  /// \param mp instance of the policy that describe how to process the message
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &MessageProcessor);

  /// \brief Disconnect the kafka consumer and destroy the TopicPartition
  /// vector.
  ///
  /// \remarks Make sure that the Streamer status is
  /// StreamerErrorCode::has_finished.
  ///
  /// \return The current status.
  StreamerStatus closeStream();

  /// \brief Sets the sources.
  ///

  /// \return The number of sources.
  size_t numSources() const { return Sources.size(); }
  void setSources(
      std::unordered_map<FlatbufferMessage::SrcHash, Source> &SourceList);

  /// \brief Removes the source from the sources list.
  ///
  /// \param SourceName The name of the source to be removed.
  ///
  /// \return True if success, else false (e.g. the source is not in the list).
  bool removeSource(FlatbufferMessage::SrcHash Hash);

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

  std::map<FlatbufferMessage::SrcHash, std::string> Sources;
  StreamerOptions Options;

  std::future<std::pair<Status::StreamerStatus, ConsumerPtr>>
      ConsumerInitialised;

private:
  bool ifConsumerIsReadyThenAssignIt();
  bool stopTimeExceeded(FileWriter::DemuxTopic &MessageProcessor);
  SharedLogger Logger = getLogger();
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

bool stopTimeElapsed(std::uint64_t MessageTimestamp,
                     std::chrono::milliseconds Stoptime,
                     SharedLogger const &Logger);
} // namespace FileWriter
