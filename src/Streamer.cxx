#include <librdkafka/rdkafkacpp.h>
#include "logger.h"

#include "Streamer.hpp"
#include "helper.h"

/// TODO:
///   - reconnect if consumer return broker error

int64_t BrightnESS::FileWriter::Streamer::step_back_amount = 100;
milliseconds BrightnESS::FileWriter::Streamer::consumer_timeout =
    milliseconds(1000);

BrightnESS::FileWriter::Streamer::Streamer(const std::string &broker,
                                           const std::string &topic_name,
                                           const RdKafkaOffset &offset,
                                           const RdKafkaPartition &partition)
    : _offset(offset), _partition(partition) {

  std::string errstr;
  {
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string debug;
    if (conf->set("metadata.broker.list", broker, errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (conf->set("fetch.message.max.bytes", "10485760", errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (conf->set("receive.message.max.bytes", "10485760", errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }

    if (!debug.empty()) {
      if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Failed to initialise configuration: " +
                                 errstr);
      }
    }
    if (!(_consumer = RdKafka::Consumer::create(conf, errstr))) {
      throw std::runtime_error("Failed to create consumer: " + errstr);
    }
    delete conf;
  }

  {
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (topic_name.empty()) {
      throw std::runtime_error("Topic required");
    }
    _topic = RdKafka::Topic::create(_consumer, topic_name, tconf, errstr);
    if (!_topic) {
      throw std::runtime_error("Failed to create topic: " + errstr);
    }
    delete tconf;
  }

  // Start consumer for topic+partition at start offset
  RdKafka::ErrorCode resp =
      _consumer->start(_topic, _partition.value(), _offset.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
  }
  int64_t high, low;
  _consumer->query_watermark_offsets(topic_name, _partition.value(), &low,
                                     &high, 1000);
  LOG(5, "Offset low : {}\t high : {}", low, high);
  _low = RdKafkaOffset(low);
  if (_offset.value() == RdKafka::Topic::OFFSET_END) {
    _offset = RdKafkaOffset(high);
  } else {
    if (_offset.value() == RdKafka::Topic::OFFSET_BEGINNING) {
      _offset = _low;
    }
  }
  _begin = _offset;
}

BrightnESS::FileWriter::Streamer::Streamer(const Streamer &other)
    : _topic(other._topic), _consumer(other._consumer), _offset(other._offset),
      _begin(other._offset), _low(other._low), _partition(other._partition) {}

BrightnESS::FileWriter::ErrorCode
BrightnESS::FileWriter::Streamer::disconnect() {
  BrightnESS::FileWriter::ErrorCode return_code =
      _consumer->stop(_topic, _partition.value());
  delete _topic;
  delete _consumer;
  return return_code;
}

BrightnESS::FileWriter::ErrorCode
BrightnESS::FileWriter::Streamer::closeStream() {
  return ErrorCode(_consumer->stop(_topic, _partition.value()));
}

int BrightnESS::FileWriter::Streamer::connect(
    const std::string &broker, const std::string &topic_name,
    const RdKafkaOffset &offset, const RdKafkaPartition &partition) {
  (*this) = std::move(Streamer(broker, topic_name, offset, partition));
  return int(RdKafka::ERR_NO_ERROR);
}

BrightnESS::FileWriter::RdKafkaOffset
BrightnESS::FileWriter::Streamer::jump_back_impl(const int &amount) {
  _offset = RdKafkaOffset(std::max(_begin.value() - amount, _low.value()));
  auto err = _consumer->seek(_topic, _partition.value(), _offset.value(), 100);
  if (err != RdKafka::ERR_NO_ERROR) {
    LOG(2, "seek failed :\t{}", RdKafka::err2str(err));
  }
  RdKafka::Message *msg;
  do {
    msg = _consumer->consume(_topic, _partition.value(),
                             consumer_timeout.count());
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
  RdKafka::Message *msg =
      _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
  if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    LOG(3, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    return ProcessMessageResult::ERR();
  }
  message_length = msg->len();
  _offset = RdKafkaOffset(msg->offset());

  auto result = mp.process_message((char *)msg->payload(), msg->len());
  return result;
}

template <>
BrightnESS::FileWriter::RdKafkaOffset
BrightnESS::FileWriter::Streamer::scan_timestamps<>(
    BrightnESS::FileWriter::DemuxTopic &mp,
    std::map<std::string, int64_t> &ts_list, const ESSTimeStamp &ts) {
  RdKafka::Message *msg;
  do {
    msg = _consumer->consume(_topic, _partition.value(),
                             consumer_timeout.count());
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      LOG(4, "Failed to consume message: {}", RdKafka::err2str(msg->err()));
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

template <>
BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::write<>(
    std::function<ProcessMessageResult(void *, int)> &f) {
  RdKafka::Message *msg =
      _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
  if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    LOG(3, "Failed to consume :\t{}", RdKafka::err2str(msg->err()));
    return ProcessMessageResult::ERR();
  }
  message_length = msg->len();
  _offset = RdKafkaOffset(msg->offset());
  return f(msg->payload(), msg->len());
}

template <>
BrightnESS::FileWriter::RdKafkaOffset
BrightnESS::FileWriter::Streamer::scan_timestamps<>(
    std::function<
        BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void *, int)> &f,
    std::map<std::string, int64_t> &ts_list, const ESSTimeStamp &ts) {
  RdKafka::Message *msg;
  do {
    msg = _consumer->consume(_topic, _partition.value(),
                             consumer_timeout.count());
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      LOG(4, "Failed to consume message: {}", RdKafka::err2str(msg->err()));
      break;
    }
    DemuxTopic::DT t = f((char *)msg->payload(), msg->len());
    if (t.dt > ts.count()) {
      return BrightnESS::FileWriter::RdKafkaOffset(-1);
    }
    // if (!ts_list[t.sourcename]) {
    //   ts_list[t.sourcename] = t.dt;
    auto it = ts_list.find(t.sourcename);
    if (it == ts_list.end()) {
      ts_list[t.sourcename] = t.dt;
      return BrightnESS::FileWriter::RdKafkaOffset(msg->offset());
    }
  } while (BrightnESS::FileWriter::RdKafkaOffset(msg->offset()) != _begin);
  return BrightnESS::FileWriter::RdKafkaOffset(-1);
}

template <>
std::map<std::string, int64_t>
BrightnESS::FileWriter::Streamer::set_start_time<>(
    std::function<TimeDifferenceFromMessage_DT(void *, int)> &f,
    const ESSTimeStamp tp) {
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
  pos = scan_timestamps(f, m, tp);

  if ((pos != BrightnESS::FileWriter::RdKafkaOffset(-1)) && (_offset != _low)) {
    auto err =
        _consumer->seek(_topic, _partition.value(), _offset.value(), 100);
    if (err != RdKafka::ERR_NO_ERROR) {
      LOG(3, "seek failed :\t{}", RdKafka::err2str(err));
    }
    return m;
  }
  return set_start_time(f, tp);
}
