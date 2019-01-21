/// \file
/// \brief This file contains the declaration of the Streamer class, which
/// consumes kafka logs and calls the write procedure

#pragma once

#include "DemuxTopic.h"
#include "EventLogger.h"
#include "Status.h"
#include "StreamerI.h"
#include "StreamerOptions.h"
#include "logger.h"

#include "KafkaW/KafkaW.h"

#include <chrono>
#include <future>

namespace FileWriter {
using ConsumerPtr = std::unique_ptr<KafkaW::Consumer>;

/// \brief Connect to kafka topics eventually at a given point in time
/// and consume messages.
class Streamer : public IStreamer {
  using StreamerStatus = Status::StreamerStatus;

public:
  Streamer() = default;

  /// \brief Create an instance of Streamer.
  ///
  /// \param broker Broker name or address of one of the brokers in the
  /// partition.
  ///
  /// \param TopicName Name of the topic to consume.
  /// \param Opts Opts configuration options for the streamer and
  /// RdKafka.
  Streamer(const std::string &Broker, const std::string &TopicName,
           const FileWriter::StreamerOptions &Opts);
  Streamer(const Streamer &) = delete;
  Streamer(Streamer &&other) = delete;

  ~Streamer() override = default;

  /// \brief Method that process a message.
  ///
  /// \param mp instance of the policy that describe how to process the message.
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &MessageProcessor) override;

  /// \brief Return a reference to the Options that has been set for the current
  /// Streamer.
  ///
  /// The method can be used to change the current values.
  StreamerOptions &getOptions() { return Options; }

protected:
  ConsumerPtr Consumer;
  KafkaW::BrokerSettings Settings;

  StreamerOptions Options;
  std::future<std::pair<Status::StreamerStatus, ConsumerPtr>> ConsumerCreated;
};

/// \brief Create a consumer with options specified in the class
/// constructor. Connects to the topic, eventually at the specified timestamp.
///
/// \param TopicName The topic to consume.
///
/// \return If the connection is successful returns ``SEC::writing``. If the
/// consumer can't be created returns ``SEC::configuration_error``, if the topic
/// is not in the partition ``SEC::topic_partition_error``;
std::pair<Status::StreamerStatus, ConsumerPtr>
createConsumer(std::string const &TopicName, StreamerOptions const &Options);
bool stopTimeElapsed(std::uint64_t MessageTimestamp,
                     std::chrono::milliseconds Stoptime);
} // namespace FileWriter
