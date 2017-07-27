#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <string>

#include "DemuxTopic.h"
#include "utils.h"

// forward definitions
namespace RdKafka {
class Conf;
class KafkaConsumer;
class Metadata;
class TopicPartition;
} // namespace RdKafka

namespace FileWriter {

struct Streamer {
  using status_type = std::map<std::string, int32_t>;

  Streamer(){};
  Streamer(const std::string &, const std::string &,
           std::vector<std::pair<std::string, std::string>> kafka_options = {});
  Streamer(const Streamer &);

  ~Streamer() = default;

  template <class T> ProcessMessageResult write(T &f) {
    std::cout << "fake_recv\n";
    return ProcessMessageResult::ERR();
  }

  int connect(const std::string &topic,
              const RdKafkaOffset & = RdKafkaOffsetEnd,
              const RdKafkaPartition & = RdKafkaPartition(0));

  ErrorCode closeStream();

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
  status_type &status();

private:
  RdKafka::KafkaConsumer *_consumer{nullptr};
  std::shared_ptr<RdKafka::Metadata> _metadata;
  std::vector<RdKafka::TopicPartition *> _tp;
  RdKafkaOffset _offset{RdKafkaOffsetEnd};
  RdKafkaOffset _begin;
  std::vector<RdKafkaOffset> _low;
  status_type status_;

  int32_t message_length_{0};
  int32_t n_messages_{0};
  int32_t n_sources_{0};
  ESSTimeStamp _timestamp_delay{3000};
  milliseconds consumer_timeout{1000};

  // sets options for the Streamer object
  bool set_streamer_opt(const std::pair<std::string, std::string> &opt);

  // sets Kafka configuration options
  bool set_conf_opt(std::shared_ptr<RdKafka::Conf> conf,
                    const std::pair<std::string, std::string> &option);

  // retrieve Metadata and fills TopicPartition. Retries <retry> times
  int get_metadata(int retry = 10);
  int get_topic_partitions(const std::string &topic);

  FileWriter::ErrorCode get_offset_boundaries();
};

template <> ProcessMessageResult Streamer::write<>(FileWriter::DemuxTopic &);

template <>
std::map<std::string, int64_t>
Streamer::set_start_time<>(FileWriter::DemuxTopic &, const ESSTimeStamp);
} // namespace FileWriter
