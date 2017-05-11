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
// class PartitionMetadata;
class TopicPartition;
} // namespace RdKafka

namespace BrightnESS {
namespace FileWriter {

struct Streamer {

  Streamer() {};
  Streamer(
      const std::string &, const std::string &,
      std::vector<std::pair<std::string, std::string> > kafka_options = {});
  Streamer(const Streamer &);

  ~Streamer() = default;

  template <class T> ProcessMessageResult write(T &f) {
    message_length = 0;
    std::cout << "fake_recv\n";
    return ProcessMessageResult::ERR();
  }

  int connect(const std::string &topic,
              const RdKafkaOffset & = RdKafkaOffsetEnd,
              const RdKafkaPartition & = RdKafkaPartition(0));

  ErrorCode closeStream();

  /// Returns message length
  size_t &len() { return message_length; }

  template <class T>
  std::map<std::string, int64_t> set_start_time(T &x, const ESSTimeStamp tp) {
    std::cout << "no initial timepoint\n";
    return std::map<std::string, int64_t>();
  }

  template <class T>
  BrightnESS::FileWriter::RdKafkaOffset
  scan_timestamps(T &x, std::map<std::string, int64_t> &m,
                  const ESSTimeStamp &ts) {
    std::cout << "no scan\n";
    return RdKafkaOffset(-1);
  }

  int n_sources{0};
  ErrorCode status{StatusCode::STOPPED};

private:
  RdKafka::KafkaConsumer *_consumer{ nullptr };
  std::shared_ptr<RdKafka::Metadata> _metadata;
  std::vector<std::string> _topics;
  std::vector<RdKafka::TopicPartition *> _tp;

  RdKafkaOffset _offset{ RdKafkaOffsetEnd };
  RdKafkaOffset _begin;
  std::vector<RdKafkaOffset> _low;
  int64_t step_back_offset;
  size_t message_length{ 0 };
  ESSTimeStamp _timestamp_delay{ 3000 };

  // sets options for the Streamer object
  bool set_streamer_opt(const std::pair<std::string, std::string> &opt);

  // sets Kafka configuration options
  bool set_conf_opt(std::shared_ptr<RdKafka::Conf> conf,
                    const std::pair<std::string, std::string> &option);

  // retrieve Metadata and fills TopicPartition. Retries <retry> times
  int get_metadata(int retry = 10);
  int get_topic_partitions(const std::string &topic);

  BrightnESS::FileWriter::ErrorCode get_offset_boundaries();

  milliseconds consumer_timeout{ 1000 };
  int64_t step_back_amount{ 100 };

  BrightnESS::FileWriter::RdKafkaOffset jump_back_impl(const int &);
};

template <>
ProcessMessageResult Streamer::write<>(BrightnESS::FileWriter::DemuxTopic &);

template <>
BrightnESS::FileWriter::RdKafkaOffset
Streamer::scan_timestamps<>(BrightnESS::FileWriter::DemuxTopic &,
                            std::map<std::string, int64_t> &,
                            const ESSTimeStamp &);

template <>
std::map<std::string, int64_t>
Streamer::set_start_time<>(BrightnESS::FileWriter::DemuxTopic &,
                           const ESSTimeStamp);
} // namespace FileWriter
} // namespace BrightnESS
