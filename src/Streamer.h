//===-- src/Streamer.h - Stream consumer class definition -------*- C++ -*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the declaration of the Streamer class, which
/// consumes kafka logs and calls the write procedure
///
//===----------------------------------------------------------------------===//

#pragma once

#include "DemuxTopic.h"
#include "EventLogger.h"
#include "Status.h"
#include "StreamerOptions.h"
#include "logger.h"

#include "KafkaW/KafkaW.h"

#include <chrono>
#include <future>

namespace FileWriter {
using ConsumerPtr = std::unique_ptr<KafkaW::Consumer>;

/// Connect to kafka topics eventually at a given point in time
/// and consume messages
class Streamer {
  using StreamerStatus = Status::StreamerStatus;

public:
  Streamer() = default;

  //----------------------------------------------------------------------------
  /// @brief      Constructor
  ///
  /// @param[in]  broker      Broker name or address of one of the brokers in
  /// the partition
  /// @param[in]  topic_name  Name of the topic to consume
  /// @param[in]  Opts        Opts configuration options for the streamer and
  /// RdKafka
  ///
  /// @remark     Throws an exception if fails (e.g. missing broker or topic)
  ///
  Streamer(const std::string &Broker, const std::string &TopicName,
           const FileWriter::StreamerOptions &Opts);
  Streamer(const Streamer &) = delete;
  Streamer(Streamer &&other) = default;

  ~Streamer() = default;

  /// Method that process a message
  /// \param mp instance of the policy that describe how to process the message
  ProcessMessageResult pollAndProcess(FileWriter::DemuxTopic &MessageProcessor);

  /// Disconnect the kafka consumer and destroy the TopicPartition vector. Make
  /// sure that the Streamer status is StreamerErrorCode::has_finished
  StreamerStatus closeStream();

  //----------------------------------------------------------------------------
  /// @brief      Return the number of different sources whose last message is
  /// not older than the stop time
  ///
  /// @return     The number of sources
  ///
  const size_t numSources() { return Sources.size(); }
  void setSources(std::unordered_map<std::string, Source> &SourceList);
  //----------------------------------------------------------------------------
  /// @brief      Removes the source from the sources list.
  ///
  /// @param[in]  SourceName  The name of the source to be removed
  ///
  /// @return     True if success, else false (e.g. the source is not in the
  /// list)
  ///
  bool removeSource(const std::string &SourceName);

  //----------------------------------------------------------------------------
  /// @brief      Returns the status of the Streamer. See "Error.h"
  ///
  /// @return     The current status
  ///
  StreamerStatus &runStatus() { return RunStatus; }

  /// Return all the informations about the messages consumed
  Status::MessageInfo &messageInfo() { return MessageInfo; }

  /// Return a reference to the Options that has been set for the current
  /// Streamer. The method can be used to change the current values
  StreamerOptions &getOptions() { return Options; }

  void setEventLogger(std::shared_ptr<EventLogger> Logger) {
    EventLog = Logger;
  }

protected:
  ConsumerPtr Consumer;
  KafkaW::BrokerSettings Settings;

  StreamerStatus RunStatus{StreamerStatus::NOT_INITIALIZED};
  Status::MessageInfo MessageInfo;

  std::vector<std::string> Sources;
  StreamerOptions Options;

  std::future<std::pair<Status::StreamerStatus, ConsumerPtr>> ConsumerCreated;
  std::shared_ptr<EventLogger> EventLog{nullptr};
};

//----------------------------------------------------------------------------
/// @brief      Create a consumer with the options specified in the class
/// constructor. Conncts to the topic, eventualli at the specified timestamp.
///
/// @param[in]  TopicName  The topic to consume
///
/// @return     If the connection is successful returns ``SEC::writing``. If
/// the
/// consumer can't be created returns ``SEC::configuration_error``, if the
/// topic is
/// not in the partition ``SEC::topic_partition_error``;
///
std::pair<Status::StreamerStatus, ConsumerPtr>
createConsumer(std::string const TopicName, StreamerOptions const Options);
bool stopTimeElapsed(std::uint64_t MessageTimestamp,
                     std::chrono::milliseconds Stoptime);
} // namespace FileWriter
