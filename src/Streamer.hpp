//===-- src/Streamer.hpp - Stream consumer class definition -------*- C++ -*-===//
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

/// Class that connects to kafka topics eventually at a given point in time
/// and consumes messages, usually writing them on the disk
class Streamer {
  friend class ::StreamerTest;
public:
  using SEC = Status::StreamerErrorCode;

  Streamer(){};
  Streamer(const std::string &, const std::string &, const FileWriter::StreamerOptions&);
  Streamer(const Streamer &) = delete;
  Streamer(Streamer &&other) = default;

  ~Streamer();

  template <class T> ProcessMessageResult write(T &f) {
    LOG(0,"fake_recv");
    return ProcessMessageResult::ERR();
  }

  SEC close_stream();

  int32_t &n_sources() { return n_sources_; }
  SEC remove_source();

  const SEC &runstatus() { return run_status_; }

  Status::MessageInfo &info() { return message_info_; }

private:
  std::shared_ptr<RdKafka::KafkaConsumer> consumer;
  std::vector<RdKafka::TopicPartition *> _tp;

  SEC run_status_{};
  Status::MessageInfo message_info_;

  std::thread connect_;
  std::mutex connection_ready_;

  std::mutex connection_lock_;
  std::condition_variable connection_init_;
  std::atomic<bool> initilialising_{false};

  int32_t n_sources_{0};
  StreamerOptions Options;
  
  void connect(const std::string &broker, const FileWriter::StreamerOptions& options);
  std::unique_ptr<RdKafka::Conf> create_configuration(const FileWriter::StreamerOptions& options);
  SEC create_consumer(std::unique_ptr<RdKafka::Conf> &&);
  std::unique_ptr<RdKafka::Metadata> create_metadata();
  SEC create_topic_partition(const std::string &topic,
                             std::unique_ptr<RdKafka::Metadata> &&);
  void push_topic_partition(const std::string &topic, const int32_t &partition);
  SEC assign_topic_partition();
};

template <> ProcessMessageResult Streamer::write<>(FileWriter::DemuxTopic &);

} // namespace FileWriter
