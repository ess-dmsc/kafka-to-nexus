#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <string>

#include "DemuxTopic.h"
#include "utils.h"

// forward definitions
namespace RdKafka {
class Topic;
class Consumer;
class TopicPartition;
class Message;
} // namespace RdKafka

namespace BrightnESS {
namespace FileWriter {

// actually a "kafka streamer"
struct Streamer {
  static milliseconds consumer_timeout;
  static int64_t step_back_amount;

  Streamer() {};
  Streamer(const std::string &, const std::string &,
           const RdKafkaOffset & = RdKafkaOffsetEnd,
           const RdKafkaPartition & = RdKafkaPartition(0));
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

  ProcessMessageResult get_offset();

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

private:
  RdKafka::Topic *_topic{ nullptr };
  RdKafka::Consumer *_consumer{ nullptr };
  RdKafka::TopicPartition *_tp;
  RdKafkaOffset _offset;
  RdKafkaOffset _begin;
  RdKafkaOffset _low;
  int64_t step_back_offset;
  RdKafkaPartition _partition;
  size_t message_length{ 0 };

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
