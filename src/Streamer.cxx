#include "logger.h"
#include <librdkafka/rdkafkacpp.h>

#include "Streamer.hpp"
#include "helper.h"

int64_t BrightnESS::FileWriter::Streamer::step_back_amount = 100;
milliseconds BrightnESS::FileWriter::Streamer::consumer_timeout =
    milliseconds(1000);

int BrightnESS::FileWriter::Streamer::set_conf_opt(
    std::shared_ptr<RdKafka::Conf> conf,
    const std::pair<std::string, std::string> &option) {
  std::string errstr;
  if (!(option.first.empty() || option.second.empty())) {
    auto result = conf->set(option.first, option.second, errstr);
    LOG(5, "set streamer config: {} = {}", option.first, option.second);
    if (result != RdKafka::Conf::CONF_OK) {
      LOG(3, "Failed to initialise configuration: {}", errstr);
      return result;
    }
  }
  return RdKafka::Conf::CONF_OK;
}

bool BrightnESS::FileWriter::Streamer::set_streamer_opt(
    const std::pair<std::string, std::string> &opt) {
  if (opt.first == "start.offset") {
    if (opt.second == "beginning") {
      _offset = RdKafkaOffsetBegin;
      return true;
    } else {
      if (opt.second == "end") {
        _offset = RdKafkaOffsetEnd;
        return true;
      } else {
        auto value = to_num<int>(opt.second);
        if (value.first && value.second > 0) {
          _offset = RdKafkaOffset(RdKafka::Consumer::OffsetTail(value.second));
          return true;
        }
      }
    }
  }

  if (opt.first == "partition") {
    auto value = to_num<int>(opt.second);
    if (value.first && value.second > 0) {
      _partition = RdKafkaPartition(value.second);
      return true;
    }
  }

  return false;
}

BrightnESS::FileWriter::Streamer::Streamer(
    const std::string &broker, const std::string &topic_name,
    const std::vector<std::pair<std::string, std::string>> &kafka_options) {

  for (auto &item : kafka_options) {
    set_streamer_opt(item);
  }

  std::string errstr;
  std::shared_ptr<RdKafka::Conf> conf(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  using opt_t = std::pair<std::string, std::string>;
  set_conf_opt(conf, opt_t{"metadata.broker.list", broker});
  set_conf_opt(conf, opt_t{"fetch.message.max.bytes", "204857600"});
  set_conf_opt(conf, opt_t{"receive.message.max.bytes", "204857600"});
  set_conf_opt(conf, opt_t{"api.version.request", "true"});
  set_conf_opt(conf, opt_t{"log_level", "3"});

  for (auto &item : kafka_options) {
    set_conf_opt(conf, item);
  }

  if (!(_consumer = RdKafka::Consumer::create(conf.get(), errstr))) {
    LOG(0, "Failed to create consumer: {}", errstr);
    exit(-1);
  }

  if (!topic_name.empty()) {
    auto value = connect(topic_name, _offset, _partition);
    if (value) {
      LOG(1, "Error: {}", value);
    }
  } else {
    LOG(3, "Topic required");
  }
}

BrightnESS::FileWriter::Streamer::Streamer(const Streamer &other)
    : _topic(other._topic), _consumer(other._consumer), _offset(other._offset),
      _begin(other._offset), _low(other._low), _partition(other._partition) {}

BrightnESS::FileWriter::ErrorCode
BrightnESS::FileWriter::Streamer::closeStream() {
  BrightnESS::FileWriter::ErrorCode status;
  if (_consumer) {
    status = _consumer->stop(_topic, _partition.value());
    delete _consumer;
  }
  if (_topic) {
    delete _topic;
  }
  return status;
}

int BrightnESS::FileWriter::Streamer::connect(
    const std::string &topic_name, const RdKafkaOffset &offset,
    const RdKafkaPartition &partition) {
  if (!_topic) {
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> tconf(
        RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
    _topic = RdKafka::Topic::create(_consumer, topic_name, tconf.get(), errstr);
    if (!_topic) {
      LOG(1, "{}", errstr);
      return int(RdKafka::ERR_TOPIC_EXCEPTION);
    }

    RdKafka::ErrorCode resp =
        _consumer->start(_topic, _partition.value(), _offset.value());
    if (resp != RdKafka::ERR_NO_ERROR) {
      return int(RdKafka::ERR_UNKNOWN);
    }
    int64_t high, low;
    _consumer->query_watermark_offsets(topic_name, _partition.value(), &low,
                                       &high, 1000);
    LOG(5, "{} -> offset low : {}\t high : {}", topic_name, low, high);
    _low = RdKafkaOffset(low);
    if (_offset.value() == RdKafka::Topic::OFFSET_END) {
      _offset = RdKafkaOffset(high);
    } else {
      if (_offset.value() == RdKafka::Topic::OFFSET_BEGINNING) {
        _offset = _low;
      }
    }
    _begin = _offset;
  } else {
    LOG(2, "Cannot connect to {}: streamer already connected ({})", topic_name,
        _topic->name());
  }

  return int(RdKafka::ERR_NO_ERROR);
}

BrightnESS::FileWriter::RdKafkaOffset
BrightnESS::FileWriter::Streamer::jump_back_impl(const int &amount) {
  _offset = RdKafkaOffset(std::max(_begin.value() - amount, _low.value()));
  auto err = _consumer->seek(_topic, _partition.value(), _offset.value(), 100);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(2, "seek failed :\t{}", RdKafka::err2str(err));
  }
  std::unique_ptr<RdKafka::Message> msg;
  do {
    msg = std::unique_ptr<RdKafka::Message>(_consumer->consume(
        _topic, _partition.value(), consumer_timeout.count()));
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      LOG(3, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    }
    _consumer->poll(10);
  } while (msg->err() != RdKafka::ERR_NO_ERROR);
  return RdKafkaOffset(msg->offset());
}

template <>
BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::write(
    BrightnESS::FileWriter::DemuxTopic &mp) {

  std::unique_ptr<RdKafka::Message> msg{
      _consumer->consume(_topic, _partition.value(), consumer_timeout.count())};

  LOG(6, "{} : event timestamp : {}", _topic->name(),
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
  message_length += msg->len();
  _offset = RdKafkaOffset(msg->offset());

  auto result = mp.process_message((char *)msg->payload(), msg->len());
  return result;
}

template <>
BrightnESS::FileWriter::RdKafkaOffset
BrightnESS::FileWriter::Streamer::scan_timestamps<>(
    BrightnESS::FileWriter::DemuxTopic &mp,
    std::map<std::string, int64_t> &ts_list, const ESSTimeStamp &ts) {
  std::unique_ptr<RdKafka::Message> msg;
  do {
    msg = std::unique_ptr<RdKafka::Message>(_consumer->consume(
        _topic, _partition.value(), consumer_timeout.count()));

    if (msg->err() != RdKafka::ERR_NO_ERROR &&
        msg->err() != RdKafka::ERR__PARTITION_EOF &&
        msg->err() != RdKafka::ERR__TIMED_OUT) {
      LOG(3, "Failed to consume message: {}", RdKafka::err2str(msg->err()));
      break;
    }
    DemuxTopic::DT t =
        mp.time_difference_from_message((char *)msg->payload(), msg->len());

    if (t.dt > ts.count()) {
      return BrightnESS::FileWriter::RdKafkaOffset(-1);
    }

    auto it = ts_list.find(t.sourcename);
    if (it == ts_list.end()) {
      ts_list[t.sourcename] = t.dt;
      if (ts_list.size() == mp.sources().size()) {
        return BrightnESS::FileWriter::RdKafkaOffset(msg->offset());
      }
    }
  } while (BrightnESS::FileWriter::RdKafkaOffset(msg->offset()) != _begin);
  return BrightnESS::FileWriter::RdKafkaOffset(-1);
}

template <>
std::map<std::string, int64_t>
BrightnESS::FileWriter::Streamer::set_start_time<>(
    BrightnESS::FileWriter::DemuxTopic &mp, const ESSTimeStamp tp) {
  std::map<std::string, int64_t> m;
  if (_offset == _low) {
    auto err = _consumer->seek(_topic, _partition.value(),
                               RdKafka::Topic::OFFSET_BEGINNING, 100);
    if (err != RdKafka::ERR_NO_ERROR) {
      LOG(3, "seek failed :\t{}", RdKafka::err2str(err));
    }
    return m;
  }
  _begin = _offset;
  BrightnESS::FileWriter::RdKafkaOffset pos = jump_back_impl(step_back_amount);
  pos = scan_timestamps(mp, m, tp);

  if ((pos != BrightnESS::FileWriter::RdKafkaOffset(-1)) && (_offset != _low)) {
    auto err =
        _consumer->seek(_topic, _partition.value(), _offset.value(), 100);
    if (err != RdKafka::ERR_NO_ERROR) {
      LOG(3, "seek failed :\t{}", RdKafka::err2str(err));
    }
    return m;
  }
  return set_start_time(mp, tp);
}
