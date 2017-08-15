#include <algorithm>
#include <memory>

#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

#include "Streamer.hpp"
#include "helper.h"
#include <unistd.h>

bool FileWriter::Streamer::set_conf_opt(
    std::shared_ptr<RdKafka::Conf> conf,
    const std::pair<std::string, std::string> &option) {
  std::string errstr;
  if (!(option.first.empty() || option.second.empty())) {
    auto result = conf->set(option.first, option.second, errstr);
    LOG(5, "set kafka config: {} = {}", option.first, option.second);
    if (result != RdKafka::Conf::CONF_OK) {
      LOG(3, "Failed to initialise configuration: {}", errstr);
      return false;
    }
  }
  return true;
}

bool FileWriter::Streamer::set_streamer_opt(
    const std::pair<std::string, std::string> &option) {

  if (option.first == "start.offset") {
    LOG(5, "set streamer config: {} = {}", option.first, option.second);
    if (option.second == "beginning") {
      _offset = RdKafkaOffsetBegin;
      return true;
    } else {
      if (option.second == "end") {
        _offset = RdKafkaOffsetEnd;
        return true;
      } else {
        auto value = to_num<int>(option.second);
        if (value.first && value.second >= 0) {
          _offset = RdKafkaOffset(RdKafka::Consumer::OffsetTail(value.second));
          return true;
        }
      }
    }
  }
  if (option.first == "timestamp_delay") {
    auto value = to_num<int>(option.second);
    if (value.first && value.second > 0) {
      _timestamp_delay = ESSTimeStamp(value.second);
      return true;
    }
  }
  return false;
}

std::unique_ptr<RdKafka::Metadata>
FileWriter::Streamer::get_metadata(int retry) {
  RdKafka::Metadata *md;
  std::unique_ptr<RdKafka::Metadata> metadata{nullptr};
  std::unique_ptr<RdKafka::Topic> ptopic;
  auto err = _consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(3, "Can't request metadata");
    // if (retry) {
    //   get_metadata(retry - 1);
    // } else {
    //   return -1;
    // }
  } else {
    metadata.reset(md);
  }
  return metadata;
}

int FileWriter::Streamer::get_topic_partitions(
    const std::string &topic, std::unique_ptr<RdKafka::Metadata> _metadata) {
  bool topic_found = false;
  if (!_metadata) {
    LOG(3, "Missing metadata informations");
    return -1;
  }
  auto partition_metadata = _metadata->topics()->at(0)->partitions();
  for (auto &i : *_metadata->topics()) {
    if (i->topic() == topic) {
      partition_metadata = i->partitions();
      topic_found = true;
    }
  }
  if (!topic_found) {
    LOG(3, "Can't find topic : {}", topic);
    return -2;
  }
  for (auto &i : (*partition_metadata)) {
    _tp.push_back(RdKafka::TopicPartition::create(topic, i->id()));
  }
  return 0;
}

FileWriter::Streamer::Error FileWriter::Streamer::get_offset_boundaries() {
  for (auto &i : _tp) {
    int64_t high, low;
    auto err = _consumer->query_watermark_offsets(i->topic(), i->partition(),
                                                  &low, &high, 5000);
    if (err) {
      LOG(1, "Unable to get boundaries for topic {}, partition {} : {}",
          i->topic(), i->partition(), RdKafka::err2str(err));
      return Error(ErrorCode::topic_partition_error);
    }
    _low.push_back(RdKafkaOffset(low));
  }
  return Error(ErrorCode::no_error);
}

std::shared_ptr<RdKafka::Conf> FileWriter::Streamer::initialize_configuration(
    std::vector<option_t> &kafka_options) {

  std::shared_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  kafka_options.erase(
      std::remove_if(kafka_options.begin(), kafka_options.end(),
                     [&](std::pair<std::string, std::string> &item) -> bool {
                       return this->set_streamer_opt(item);
                     }),
      kafka_options.end());

  kafka_options.erase(
      std::remove_if(kafka_options.begin(), kafka_options.end(),
                     [&](std::pair<std::string, std::string> &item) -> bool {
                       return this->set_conf_opt(conf, item);
                     }),
      kafka_options.end());
  if (!kafka_options.empty()) {
    for (auto &item : kafka_options) {
      LOG(3, "Unknown option: {} [{}]", item.first, item.second);
    }
  }
  return conf;
}

FileWriter::Streamer::Streamer(
    const std::string &broker, const std::string &topic_name,
    std::vector<std::pair<std::string, std::string>> kafka_options) {

  s_.run_status(StreamerError(Status::StreamerErrorCode::not_initialized));
  if (topic_name.empty() || broker.empty()) {
    LOG(0, "Broker and topic required");
    s_.run_status(Error(ErrorCode::not_initialized));
    return;
  }
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"metadata.broker.list", broker});
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"api.version.request", "true"});
  kafka_options.push_back(
      FileWriter::Streamer::option_t{"group.id", topic_name});

  connect_ = std::thread([&] {
    this->connect(topic_name, kafka_options);
    return;
  });
  std::this_thread::sleep_for(milliseconds(10));
}

FileWriter::Streamer::~Streamer(){};

void FileWriter::Streamer::connect(
    const std::string topic_name,
    std::vector<FileWriter::Streamer::option_t> kafka_options) {
  std::lock_guard<std::mutex> lock(guard_);

  LOG(7, "Connecting to {}", topic_name);
  for (auto &i : kafka_options) {
    LOG(7, "{} :\t{}", i.first, i.second);
  }

  std::string errstr;
  // Initialize configuration and consumer
  auto conf = initialize_configuration(kafka_options);
  if (!conf) {
    LOG(0, "Unable to initialize configuration ({})", topic_name);
    s_.run_status(Status::StreamerErrorCode::configuration_error);
    return;
  }
  RdKafka::KafkaConsumer *c =
      RdKafka::KafkaConsumer::create(conf.get(), errstr);
  _consumer.reset(c);
  if (!_consumer) {
    LOG(0, "Failed to create consumer: {}", errstr);
    s_.run_status(Status::StreamerErrorCode::consumer_error);
    return;
  }

  // Retrieve informations
  std::unique_ptr<RdKafka::Metadata> metadata = get_metadata();
  if (!metadata) {
    LOG(1, "Unable to retrieve metadata");
    s_.run_status(Status::StreamerErrorCode::metadata_error);
    return;
  }
  if (get_topic_partitions(topic_name, std::move(metadata)) < 0) {
    LOG(1, "Unable to build TopicPartitions structure");
    s_.run_status(Status::StreamerErrorCode::topic_partition_error);
    return;
  }
  // Assign consumer
  for (auto &i : _tp) {
    i->set_offset(_offset.value());
  }
  auto err = _consumer->assign(_tp);
  if (err) {
    LOG(0, "Failed to subscribe to {} ", topic_name);
    s_.run_status(Status::StreamerErrorCode::assign_error);
    return;
  }
  auto val = get_offset_boundaries();
  if (val.value() != ErrorCode::no_error) {
    s_.run_status(val);
    return;
  }

  LOG(7, "Connected to topic {}", topic_name);
  s_.run_status(Status::StreamerErrorCode::writing);
  return;
}

FileWriter::Streamer::Error FileWriter::Streamer::closeStream() {
  std::lock_guard<std::mutex> lock(guard_);
  s_.run_status(StreamerError(Status::StreamerErrorCode::stopped));
  if (connect_.joinable()) {
    connect_.join();
  }
  if (_consumer) {
    _consumer->close();
  }
  _tp.clear();
  return Error(s_.run_status());
}

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(FileWriter::DemuxTopic &mp) {
  std::lock_guard<std::mutex> lock(
      guard_); // make sure that connect is completed

  if (s_.run_status().value() < 0) {
    return ProcessMessageResult::ERR();
  }
  if (s_.run_status().value() == ErrorCode::stopped) {
    return ProcessMessageResult::OK();
  }

  std::unique_ptr<RdKafka::Message> msg{
      _consumer->consume(consumer_timeout.count())};

  LOG(6, "{} : event timestamp : {}", _tp[0]->topic(),
      msg->timestamp().timestamp);

  if (msg->err() == RdKafka::ERR__PARTITION_EOF ||
      msg->err() == RdKafka::ERR__TIMED_OUT) {
    LOG(5, "consume :\t{}", RdKafka::err2str(msg->err()));
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    LOG(5, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    s_.error(StreamerError(Status::StreamerErrorCode::message_error));
    return ProcessMessageResult::ERR();
  }
  s_.add_message(msg->len());
  _offset = RdKafkaOffset(msg->offset());

  auto result = mp.process_message((char *)msg->payload(), msg->len());
  if (!result.is_OK()) {
    s_.run_status(Error(ErrorCode::write_error));
  }
  return result;
}

FileWriter::Streamer::Error
FileWriter::Streamer::set_start_time(const ESSTimeStamp &timepoint) {
  std::lock_guard<std::mutex> lock(guard_); // make sure connnection is done

  auto value =
      std::chrono::duration_cast<KafkaTimeStamp>(timepoint - _timestamp_delay);
  for (auto &i : _tp) {
    i->set_offset(value.count());
  }
  auto err = _consumer->offsetsForTimes(_tp, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    s_.run_status(StreamerError(Status::StreamerErrorCode::start_time_error));
    LOG(3, "Error searching initial time: {}", err2str(err));
    for (auto &i : _tp) {
      LOG(3, "TopicPartition {}-{} : {}", i->topic(), i->partition(),
          RdKafka::err2str(i->err()));
    }
    s_.run_status(Error(ErrorCode::start_time_error));
    return Error(ErrorCode::start_time_error);
  }
  if (err == RdKafka::ERR_NO_ERROR) {
    for (auto &i : _tp) {
      auto offset = i->offset();
      i->set_offset(offset);
    }
  }
  err = _consumer->assign(_tp);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(3, "Error assigning initial time: {}", err2str(err));
    s_.run_status(Error(ErrorCode::start_time_error));
    return Error(ErrorCode::start_time_error);
  }
  return Error(ErrorCode::no_error);
}
