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
#include "Status.h"
#include "StreamerOptions.h"
#include "logger.h"

#include "KafkaW/KafkaW.h"

#include <future>

class T_Streamer;

namespace FileWriter {

/// Connect to kafka topics eventually at a given point in time
/// and consume messages
class Streamer {
  friend class ::T_Streamer;

public:
  using SEC = Status::StreamerErrorCode;

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
  Streamer(const std::string &broker, const std::string &topic_name,
           const FileWriter::StreamerOptions &Opts);
  Streamer(const Streamer &) = delete;
  Streamer(Streamer &&other) = default;

  ~Streamer() = default;

  /// Generic template method that process a message according to a policy T
  /// \param mp instance of the policy that describe how to process the message
  template <class T> ProcessMessageResult write(T &mp) {
    LOG(Sev::Warning, "fake_recv");
    return ProcessMessageResult::ERR();
  }

  /// Disconnect the kafka consumer and destroy the TopicPartition vector. Make
  /// sure that the Streamer status is StreamerErrorCode::has_finished
  SEC closeStream();

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
  SEC &runStatus() { return RunStatus; }

  /// Return all the informations about the messages consumed
  Status::MessageInfo &messageInfo() { return MessageInfo; }

  /// Return a reference to the Options that has been set for the current
  /// Streamer. The method can be used to change the current values
  StreamerOptions &getOptions() { return Options; }

private:
  std::unique_ptr<KafkaW::Consumer> Consumer;
  KafkaW::BrokerSettings Settings;

  SEC RunStatus{SEC::not_initialized};
  Status::MessageInfo MessageInfo;

  std::vector<std::string> Sources;
  StreamerOptions Options;

  std::future<SEC> IsConnected;
  std::once_flag ConnectionStatus;

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
  SEC connect(std::string TopicName);
};

/// Consume a Kafka message and process it according to
/// DemuxTopic::process_message. If the message contains errors return
/// ProcessMessageResult::ERR() and increase the count of error messages in the
/// status, else ProcessMessageResult::OK(). If there are no messages within the
/// given time return a poll timeout. If a start time is set discard all the
/// messages generated earlier than the start time. If the message is correctly
/// processed update a Status object.
///
///\param MessageProcessor instance of a DemuxTopic that implements the
/// process_message
/// method.
template <>
ProcessMessageResult
Streamer::write<>(FileWriter::DemuxTopic &MessageProcessor);

} // namespace FileWriter
