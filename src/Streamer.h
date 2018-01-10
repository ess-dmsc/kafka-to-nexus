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
#include "Status.hpp"
#include "StreamerOptions.h"
#include "logger.h"

#include <condition_variable>
#include <mutex>
#include <thread>

namespace RdKafka {
class Conf;
class KafkaConsumer;
class Metadata;
class TopicPartition;
} // namespace RdKafka

class StreamerTest;

namespace FileWriter {

/// Connect to kafka topics eventually at a given point in time
/// and consume messages
class Streamer {
  friend class ::StreamerTest;

public:
  using SEC = Status::StreamerErrorCode;

  Streamer(){};
  /// Constructor
  /// \param broker name or address of one of the brokers in the partition
  /// \param topic_name name of the topic to listen for messages
  /// \param Opts configuration options for the streamer and RdKafka
  Streamer(const std::string &broker, const std::string &topic_name,
           const FileWriter::StreamerOptions &Opts);
  Streamer(const Streamer &) = delete;
  Streamer(Streamer &&other) = default;

  ~Streamer();

  /// Generic template method that process a message according to a policy T
  /// \param mp instance of the policy that describe how to process the message
  template <class T> ProcessMessageResult write(T &mp) {
    LOG(0, "fake_recv");
    return ProcessMessageResult::ERR();
  }

  /// Disconnect the kafka consumer and destroy the TopicPartition vector. Make
  /// sure that the Streamer status is StreamerErrorCode::has_finished
  SEC closeStream();

  /// Return the number of different sources in the topic whose last message is
  /// not older than the stop time (if specified)
  const size_t numSources() { return Sources.size(); }
  void setSources(std::unordered_map<std::string, Source> &SourceList);
  bool removeSource(const std::string &SourceName);

  /// Return a StreamerErrorCode that describes the status of the Streamer
  SEC &runStatus() { return RunStatus; }

  /// Return all the informations about the messages consumed
  Status::MessageInfo &messageInfo() { return MessageInfo; }

  /// Return a reference to the Options that has been set for the current
  /// Streamer. The method can be used to change the current values
  StreamerOptions &getOptions() { return Options; }

private:
  std::shared_ptr<RdKafka::KafkaConsumer> Consumer;
  std::vector<RdKafka::TopicPartition *> TopicPartitionVector;

  SEC RunStatus{};
  Status::MessageInfo MessageInfo;

  std::thread ConnectThread;
  std::mutex ConnectionReady;

  std::mutex ConnectionLock;
  std::condition_variable ConnectionInit;
  std::atomic<bool> Initilialising{false};

  int32_t NumSources{0};
  std::vector<std::string> Sources;
  StreamerOptions Options;

  void connect(const std::string &);
  std::unique_ptr<RdKafka::Conf>
  createConfiguration(const FileWriter::StreamerOptions &);
  SEC createConsumer(std::unique_ptr<RdKafka::Conf> &&);
  std::unique_ptr<RdKafka::Metadata> createMetadata();
  SEC createTopicPartition(const std::string &,
                           std::unique_ptr<RdKafka::Metadata> &&);
  void pushTopicPartition(const std::string &, const int32_t &);
  SEC assignTopicPartition();
};

/// Consume a Kafka message and process it according to
/// DemuxTopic::process_message. If the message contains errors return
/// ProcessMessageResult::ERR() and increase the count of error messages in the
/// status, else ProcessMessageResult::OK(). If there are no messages within the
/// given time return a poll timeout. If a start time is set discard all the
/// messages generated earlier than the start time. If the message is correctly
/// processed update a Status object.
///
///\param mp instance of a DemuxTopic that implements the process_message
/// method.
template <> ProcessMessageResult Streamer::write<>(FileWriter::DemuxTopic &);

} // namespace FileWriter
