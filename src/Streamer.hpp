#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <string>

#include "DemuxTopic.h"
#include "utils.h"

// forward definitions
namespace RdKafka {
class KafkaConsumer;
class Conf;
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
  std::vector<std::string> _topics;

  RdKafkaOffset _offset{ RdKafkaOffsetEnd };
  RdKafkaOffset _begin;
  RdKafkaOffset _low;
  int64_t step_back_offset;
  RdKafkaPartition _partition;
  size_t message_length{ 0 };

  bool set_streamer_opt(const std::pair<std::string, std::string> &opt);
  bool set_conf_opt(std::shared_ptr<RdKafka::Conf> conf,
                    const std::pair<std::string, std::string> &option);

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
