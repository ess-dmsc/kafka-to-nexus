#include <algorithm>

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

int FileWriter::Streamer::get_metadata(int retry) {
  RdKafka::Metadata *md;
  std::unique_ptr<RdKafka::Topic> ptopic;
  auto err = _consumer->metadata(ptopic.get() != NULL, ptopic.get(), &md, 5000);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(3, "Can't request metadata");
    if (retry) {
      get_metadata(retry - 1);
    } else {
      return -1;
    }
  }
  _metadata.reset(md);
  return !_metadata;
}

int FileWriter::Streamer::get_topic_partitions(const std::string &topic) {
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

FileWriter::ErrorCode FileWriter::Streamer::get_offset_boundaries() {
  for (auto &i : _tp) {
    int64_t high, low;
    auto err = _consumer->query_watermark_offsets(i->topic(), i->partition(),
                                                  &low, &high, 5000);
    if (err) {
      LOG(1, "Unable to get topic boundaries (topic : {}, partition : {})",
          i->topic(), i->partition())
    }
    _low.push_back(RdKafkaOffset(low));
  }
  return FileWriter::ErrorCode(0);
}

FileWriter::Streamer::Streamer(
    const std::string &broker, const std::string &topic_name,
    std::vector<std::pair<std::string, std::string>> kafka_options) {

  kafka_options.erase(
      std::remove_if(kafka_options.begin(), kafka_options.end(),
                     [&](std::pair<std::string, std::string> &item) -> bool {
                       return this->set_streamer_opt(item);
                     }),
      kafka_options.end());

  status_["status.run"] = 0;
  std::string errstr;
  std::shared_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  using opt_t = std::pair<std::string, std::string>;
  set_conf_opt(conf, opt_t{"metadata.broker.list", broker});
  set_conf_opt(conf, opt_t{"fetch.message.max.bytes", "2048576000"});
  set_conf_opt(conf, opt_t{"receive.message.max.bytes", "2048576000"});
  set_conf_opt(conf, opt_t{"api.version.request", "true"});
  set_conf_opt(conf, opt_t{"log_level", "3"});
  set_conf_opt(conf, opt_t{"group.id", topic_name});

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

  if (!(_consumer = RdKafka::KafkaConsumer::create(conf.get(), errstr))) {
    LOG(0, "Failed to create consumer: {}", errstr);
    status_.emplace("status.consumer", -1);
  } else {
    status_.emplace("status.consumer", 1);
  }

  if (!topic_name.empty()) {
    status_.emplace("status.metadata", 1);
    if (get_metadata() != 0) {
      LOG(1, "Unable to retrieve metadata");
      status_["status.metadata"] = -1;
    }
    status_.emplace("status.topic_partition", 1);
    if (get_topic_partitions(topic_name) < 0) {
      LOG(1, "Unable to build TopicPartitions structure");
      status_["status.topic_partition"] = -1;
    }
    for (auto &i : _tp) {
      i->set_offset(_offset.value());
    }

    RdKafka::ErrorCode err = _consumer->assign(_tp);
    if (err) {
      LOG(0, "Failed to subscribe to {} ", topic_name);
      status_["status.consumer"] = -1;
    }
  } else {
    LOG(0, "Topic required");
    status_["status.consumer"] = -1;
  }
  if (get_offset_boundaries().value()) {
    LOG(3, "Unable to determine lower and higher offset in topic {}",
        topic_name);
    status_["status.metadata"] = -1;
  }

  // prepare status info
  status_.emplace("status.size", 0);
  status_.emplace("status.n_messages", 0);
  status_.emplace("status.n_sources", 0);
}

FileWriter::Streamer::Streamer(const Streamer &other)
    : _consumer(other._consumer), _tp(other._tp), _offset(other._offset),
      _begin(other._offset), _low(other._low), status_(other.status_) {}

FileWriter::ErrorCode FileWriter::Streamer::closeStream() {
  FileWriter::ErrorCode status;

  _tp.clear();
  status_["status.topic_partition"] = 0;
  if (_consumer) {
    _consumer->close();
    status_["status.consumer"] = 0;
    delete _consumer;
  }
  return status;
}

int FileWriter::Streamer::connect(const std::string &topic_name,
                                  const RdKafkaOffset &offset,
                                  const RdKafkaPartition &partition) {
  // if (!_topic) {
  //   std::string errstr;
  //   std::unique_ptr<RdKafka::Conf> tconf(
  //       RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  //   _topic = RdKafka::Topic::create(_consumer, topic_name, tconf.get(),
  // errstr);
  //   if (!_topic) {
  //     LOG(1, "{}", errstr);
  //     return int(RdKafka::ERR_TOPIC_EXCEPTION);
  //   }

  //   RdKafka::ErrorCode resp =
  //       _consumer->start(_topic, _partition.value(), _offset.value());
  //   if (resp != RdKafka::ERR_NO_ERROR) {
  //     return int(RdKafka::ERR_UNKNOWN);
  //   }
  //   int64_t high, low;
  //   _consumer->query_watermark_offsets(topic_name, _partition.value(),
  // &low,
  //                                      &high, 1000);
  //   LOG(5, "{} -> offset low : {}\t high : {}", topic_name, low, high);
  //   _low = RdKafkaOffset(low);
  //   if (_offset.value() == RdKafka::Topic::OFFSET_END) {
  //     _offset = RdKafkaOffset(high);
  //   } else {
  //     if (_offset.value() == RdKafka::Topic::OFFSET_BEGINNING) {
  //       _offset = _low;
  //     }
  //   }
  //   _begin = _offset;
  // } else {
  //   LOG(2, "Cannot connect to {}: streamer already connected ({})",
  // topic_name,
  //       _topic->name());
  // }

  return int(RdKafka::ERR_UNKNOWN);
  // return int(RdKafka::ERR_NO_ERROR);
}

template <>
FileWriter::ProcessMessageResult
FileWriter::Streamer::write(FileWriter::DemuxTopic &mp) {

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
    return ProcessMessageResult::ERR();
  }
  message_length_ += msg->len();
  n_messages_++;
  _offset = RdKafkaOffset(msg->offset());

  auto result = mp.process_message((char *)msg->payload(), msg->len());
  return result;
}

template <>
std::map<std::string, int64_t>
FileWriter::Streamer::set_start_time<>(FileWriter::DemuxTopic &mp,
                                       const ESSTimeStamp timepoint) {
  std::map<std::string, int64_t> m;

  for (auto &i : _tp) {
    i->set_offset(timepoint.count() - _timestamp_delay.count());
  }
  auto err = _consumer->offsetsForTimes(_tp, 1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(3, "Error searching initial time: {}", err2str(err));
  }
  if (err == RdKafka::ERR_NO_ERROR) {
    // for (auto &i : _low) {
    //   fprintf(stderr, "Lower timestamp in partition : %ld\n", i.value());
    // }
    for (auto &i : _tp) {
      auto offset = i->offset();
      // fprintf(stderr, "Found offset : %ld\n", offset);
      i->set_offset(offset);
    }
  }
  _consumer->assign(_tp);

  return m;
}

FileWriter::Streamer::status_type &FileWriter::Streamer::status() {
  status_["status.size"] = message_length_;
  status_["status.n_messages"] = n_messages_;
  status_["status.n_sources"] = n_sources_;
  return status_;
}
