#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "DemuxTopic.h"
#include "Status.hpp"

// forward definitions
namespace RdKafka {
class Conf;
class KafkaConsumer;
class Metadata;
class TopicPartition;
} // namespace RdKafka

namespace FileWriter {

struct Streamer {
  using option_t = std::pair<std::string, std::string>;
  using Error = StreamerError;
  using ErrorCode = Status::StreamerErrorCode;

  Streamer(){};
  Streamer(const std::string &, const std::string &,
           std::vector<std::pair<std::string, std::string>> kafka_options = {});
  Streamer(const Streamer &) = delete;

  ~Streamer();

  template <class T> ProcessMessageResult write(T &f) {
    std::cout << "fake_recv\n";
    return ProcessMessageResult::ERR();
  }

  Error closeStream();

  template <class T>
  std::map<std::string, int64_t> set_start_time(T &x, const ESSTimeStamp tp) {
    std::cout << "no initial timepoint\n";
    return std::map<std::string, int64_t>();
  }

  int32_t &n_sources() { return n_sources_; }
  int run_status() {
    if (n_sources_ > 0) {
      return StatusCode::RUNNING;
    }
    return StatusCode::STOPPED;
  }

  Status::StreamerStatus &status() { return s_; }
  const StreamerError &runstatus() { return s_.run_status(); }

private:
  std::shared_ptr<RdKafka::KafkaConsumer> _consumer;
  std::vector<RdKafka::TopicPartition *> _tp;
  RdKafkaOffset _offset{RdKafkaOffsetEnd};
  RdKafkaOffset _begin;
  std::vector<RdKafkaOffset> _low;

  Status::StreamerStatus s_;
  std::thread connect_;
  std::mutex guard_;

  int32_t message_length_{0};
  int32_t n_messages_{0};
  int32_t n_sources_{0};
  ESSTimeStamp _timestamp_delay{3000};
  milliseconds consumer_timeout{1000};

  void connect(const std::string,
               std::vector<std::pair<std::string, std::string>> kafka_options);
  // sets options for Kafka consumer and the Streamer
  std::shared_ptr<RdKafka::Conf>
  initialize_configuration(std::vector<option_t> &);
  bool set_streamer_opt(const std::pair<std::string, std::string> &opt);
  bool set_conf_opt(std::shared_ptr<RdKafka::Conf> conf,
                    const std::pair<std::string, std::string> &option);

  // retrieve Metadata and fills TopicPartition. Retries <retry> times
  std::unique_ptr<RdKafka::Metadata> get_metadata(int retry = 5);
  int get_topic_partitions(const std::string &topic, std::unique_ptr<RdKafka::Metadata> metadata);

  Error get_offset_boundaries();
};

template <> ProcessMessageResult Streamer::write<>(FileWriter::DemuxTopic &);

template <>
std::map<std::string, int64_t>
Streamer::set_start_time<>(FileWriter::DemuxTopic &, const ESSTimeStamp);
} // namespace FileWriter
