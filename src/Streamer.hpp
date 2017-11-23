#pragma once

#include <condition_variable>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "DemuxTopic.h"
#include "Status.hpp"

namespace RdKafka {
class Conf;
class KafkaConsumer;
class Metadata;
class TopicPartition;
} // namespace RdKafka

namespace FileWriter {

class Streamer {
public:
  using option_t = std::pair<std::string, std::string>;
  using Options = std::vector<option_t>;
  using SEC = Status::StreamerErrorCode;

  Streamer(){};
  Streamer(const std::string &, const std::string &, Options kafka_options = {},
           Options filewriter_options = {});
  Streamer(const Streamer &) = delete;
  Streamer(Streamer &&other) = default;

  ~Streamer();

  template <class T> ProcessMessageResult write(T &f) {
    std::cout << "fake_recv\n";
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
  milliseconds ms_before_start_time{3000};
  milliseconds consumer_timeout{1000};
  int metadata_retry{5};
  milliseconds start_ts{0};

  void connect(const std::string &broker, const Options &kafka_options,
               const Options &filewriter_options);
  void set_streamer_options(const Options &);
  std::unique_ptr<RdKafka::Conf> create_configuration(const Options &);
  SEC create_consumer(std::unique_ptr<RdKafka::Conf> &&);
  std::unique_ptr<RdKafka::Metadata> create_metadata();
  SEC create_topic_partition(const std::string &topic,
                             std::unique_ptr<RdKafka::Metadata> &&);
  void push_topic_partition(const std::string &topic, const int32_t &partition);
  SEC assign_topic_partition();
};

template <> ProcessMessageResult Streamer::write<>(FileWriter::DemuxTopic &);

} // namespace FileWriter
