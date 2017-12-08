#include <algorithm>
#include <memory>

#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

#include "Msg.h"
#include "Streamer.hpp"
#include "helper.h"
#include <unistd.h>

void FileWriter::Streamer::set_streamer_options(
    const FileWriter::Streamer::Options &options) {

  for (auto &opt : options) {
    // Note: to_int<> returns a pair (validity of conversion,value).
    // First element notifies if the conversion is defined

    if (opt.first == "ms-before-start") {
      auto value = to_num<int>(opt.second);
      if (value.first && (value.second > 0)) {
        LOG(Sev::Debug, "{}: {}", opt.first, opt.second);
        ms_before_start_time = ESSTimeStamp(value.second);
      }
      continue;
    }
    if (opt.first == "metadata-retry") {
      auto value = to_num<int>(opt.second);
      if (value.first && (value.second > 0)) {
        LOG(Sev::Debug, "{}: {}", opt.first, opt.second);
        metadata_retry = value.second;
      }
      continue;
    }
    if (opt.first == "start-time-ms") {
      auto value = to_num<uint64_t>(opt.second);
      if (value.first && (value.second > 0)) {
        LOG(Sev::Debug, "{}: {}", opt.first, opt.second);
        start_ts = ESSTimeStamp{value.second};
      }
      continue;
    }
    if (opt.first == "consumer-timeout-ms") {
      auto value = to_num<uint64_t>(opt.second);
      if (value.first && (value.second > 0)) {
        LOG(Sev::Debug, "{}: {}", opt.first, opt.second);
        consumer_timeout_ms = milliseconds{value.second};
      }
      continue;
    }
    LOG(Sev::Warning, "Unknown option: {} [{}]", opt.first, opt.second);
  }
}

std::unique_ptr<RdKafka::Conf> FileWriter::Streamer::create_configuration(
    const FileWriter::Streamer::Options &kafka_options) {
  std::unique_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (!conf) {
    LOG(Sev::Error, "Error: invalid configuration");
    run_status_ = SEC::configuration_error;
  } else {
    std::string errstr;
    for (auto &opt : kafka_options) {
      LOG(Sev::Debug, "set kafka config: {} = {}", opt.first, opt.second);
      if (conf->set(opt.first, opt.second, errstr) != RdKafka::Conf::CONF_OK) {
        LOG(Sev::Warning, "{}", errstr);
      }
    }
  }
  return conf;
}

FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::create_consumer(std::unique_ptr<RdKafka::Conf> &&conf) {
  std::string errstr;
  consumer.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer) {
    LOG(Sev::Debug, "{}", errstr);
    return SEC::consumer_error;
  }
  return SEC::no_error;
}

std::unique_ptr<RdKafka::Metadata> FileWriter::Streamer::create_metadata() {
  RdKafka::Metadata *md;
  std::unique_ptr<RdKafka::Topic> ptopic;
  int retry{0};
  auto err = consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
  while (err != RdKafka::ERR_NO_ERROR && retry < metadata_retry) {
    err = consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
  }
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "{}", RdKafka::err2str(err));
    run_status_ = SEC::metadata_error;
    return std::unique_ptr<RdKafka::Metadata>{nullptr};
  } else {
    return std::unique_ptr<RdKafka::Metadata>{md};
  }
}

FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::create_topic_partition(
    const std::string &topic, std::unique_ptr<RdKafka::Metadata> &&metadata) {
  if (run_status_ == SEC::metadata_error || !metadata) {
    return SEC::metadata_error;
  }
  using PartitionMetadataVector =
      std::vector<const RdKafka::PartitionMetadata *>;
  const PartitionMetadataVector *pmv{nullptr};
  for (auto &t : *metadata->topics()) {
    if (t->topic() == topic) {
      pmv = t->partitions();
      break;
    }
  }
  if (!pmv) {
    LOG(0, "Error: unable to find partition for topic {}", topic);
    return SEC::topic_partition_error;
  }
  if (pmv->size()) {
    for (auto p : *pmv) {
      push_topic_partition(topic, p->id());
      if (!_tp.back()) {
        LOG(Sev::Error, "Error: unable to create partition {} for topic {}", p->id(),
            topic);
        return SEC::topic_partition_error;
      }
    }
  } else {
    LOG(Sev::Error, "Error: no partitions for topic {}", topic);
    return SEC::topic_partition_error;
  }

  return SEC::no_error;
}

void FileWriter::Streamer::push_topic_partition(const std::string &topic,
                                                const int32_t &partition) {
  if (start_ts.count()) {
    auto value = start_ts - ms_before_start_time;
    _tp.push_back(
        RdKafka::TopicPartition::create(topic, partition, value.count()));
  } else {
    _tp.push_back(RdKafka::TopicPartition::create(topic, partition,
                                                  RdKafka::Topic::OFFSET_END));
  }
}

FileWriter::Status::StreamerErrorCode
FileWriter::Streamer::assign_topic_partition() {
  if (!consumer || _tp.empty()) {
    return SEC::topic_partition_error;
  }
  auto err = consumer->offsetsForTimes(_tp, 1000);
  err = consumer->assign(_tp);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Error, "{}", RdKafka::err2str(err));
    return SEC::topic_partition_error;
  }
  return SEC::no_error;
};

FileWriter::Streamer::Streamer(const std::string &broker,
                               const std::string &topic_name,
                               FileWriter::Streamer::Options kafka_options,
                               FileWriter::Streamer::Options filewriter_options)
    : run_status_{SEC::not_initialized} {

  if (topic_name.empty() || broker.empty()) {
    LOG(Sev::Warning, "Broker and topic required");
    run_status_ = SEC::not_initialized;
    return;
  }
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"metadata.broker.list", broker});
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"api.version.request", "true"});
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"group.id", topic_name});

  connect_ = std::thread([&] {
    this->connect(std::ref(topic_name), std::ref(kafka_options),
                  std::ref(filewriter_options));
    return;
  });

  std::unique_lock<std::mutex> lk(connection_lock_);
  connection_init_.wait(lk, [&] { return this->initilialising_.load(); });
}

FileWriter::Streamer::~Streamer() { close_stream(); }

void FileWriter::Streamer::connect(
    const std::string &topic_name,
    const FileWriter::Streamer::Options &kafka_options,
    const FileWriter::Streamer::Options &filewriter_options) {
  std::lock_guard<std::mutex> lock(connection_ready_);

  std::lock_guard<std::mutex> lk(connection_lock_);
  initilialising_ = true;
  connection_init_.notify_all();

  LOG(Sev::Info, "Connecting to {}", topic_name);
  for (auto &i : filewriter_options) {
    LOG(Sev::Debug, "{} :\t{}", i.first, i.second);
  }
  for (auto &i : kafka_options) {
    LOG(Sev::Debug, "{} :\t{}", i.first, i.second);
  }

  set_streamer_options(filewriter_options);

  auto config = create_configuration(kafka_options);
  if (!config) {
    return;
  }
  run_status_ = create_consumer(std::move(config));
  if (run_status_ != SEC::no_error) {
    return;
  }
  auto metadata = create_metadata();
  if (!metadata) {
    return;
  }
  run_status_ = create_topic_partition(topic_name, std::move(metadata));
  if (run_status_ != SEC::no_error) {
    return;
  }
  run_status_ = assign_topic_partition();
  if (run_status_ != SEC::no_error) {
    return;
  }

  LOG(Sev::Debug, "Connected to topic {}", topic_name);
  run_status_ = SEC::writing;
}

FileWriter::Streamer::SEC FileWriter::Streamer::close_stream() {
  std::lock_guard<std::mutex> lock(connection_ready_);
  if (connect_.joinable()) {
    connect_.join();
  }
  if (consumer) {
    consumer->close();
  }
  _tp.clear();
  if (run_status_ == SEC::writing) {
    run_status_ = SEC::has_finished;
  }
  return run_status_;
}

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(FileWriter::DemuxTopic &mp) {

  if (run_status_ == SEC::not_initialized) {
    return ProcessMessageResult::OK();
  }
  std::lock_guard<std::mutex> lock(
      connection_ready_); // make sure that connect is completed

  if (int(run_status_) < 0) {
    return ProcessMessageResult::ERR();
  }
  if (run_status_ == SEC::has_finished) {
    return ProcessMessageResult::OK();
  }

  std::unique_ptr<RdKafka::Message> msg{
      consumer->consume(consumer_timeout_ms.count())};

  if (msg->err() == RdKafka::ERR__PARTITION_EOF ||
      msg->err() == RdKafka::ERR__TIMED_OUT) {
    LOG(Sev::Debug, "consume :\t{}", RdKafka::err2str(msg->err()));
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    LOG(Sev::Warning, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    message_info_.error();
    return ProcessMessageResult::ERR();
  }

  // skip message if timestamp < start_time
  auto td = mp.time_difference_from_message((char *)msg->payload(), msg->len());
  if (td.dt < start_ts.count()) {
    return ProcessMessageResult::OK();
  }

  message_info_.message(msg->len());

  auto result = mp.process_message(Msg::rdkafka(std::move(msg)));
  LOG(Sev::Debug, "{} : Message timestamp : {}", _tp[0]->topic(), result.ts());
  if (!result.is_OK()) {
    message_info_.error();
  }
  return result;
}
