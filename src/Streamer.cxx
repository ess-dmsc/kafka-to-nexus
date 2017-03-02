#include "Streamer.hpp"
#include <librdkafka/rdkafkacpp.h>
#include "logger.h"
// #include "KafkaMock.hpp"

/// TODO:
///   - reconnect if consumer return broker error
///   - search backward at connection setup

int64_t BrightnESS::FileWriter::Streamer::step_back_amount = 1000;
milliseconds BrightnESS::FileWriter::Streamer::consumer_timeout = milliseconds(1000);

BrightnESS::FileWriter::Streamer::Streamer(const std::string &broker,
                                           const std::string &topic_name,
                                           const RdKafkaOffset &offs,
                                           const RdKafkaPartition &p)
    : offset(offs), partition(p) {

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  std::string debug, errstr;
  if (conf->set("metadata.broker.list", broker, errstr) !=
      RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("Failed to initialise configuration: " + errstr);
  }
  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
  }

  conf->set("fetch.message.max.bytes", "1000000000", errstr);
  conf->set("receive.message.max.bytes", "1000000000", errstr);

  if (topic_name.empty()) {
    throw std::runtime_error("Topic required");
  }

  if (!(consumer = RdKafka::Consumer::create(conf, errstr))) {
    throw std::runtime_error("Failed to create consumer: " + errstr);
  }
  topic = RdKafka::Topic::create(consumer, topic_name, tconf, errstr);
  if (!topic) {
    throw std::runtime_error("Failed to create topic: " + errstr);
  }

  // Start consumer for topic+partition at start offset
  RdKafka::ErrorCode resp = consumer->start(topic, partition.value(), offset.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
  }

  // sets the current offset
  if (offset != RdKafka::Topic::OFFSET_END) {
    get_last_offset();
  }
}

BrightnESS::FileWriter::Streamer::Streamer(const Streamer &other)
    : topic(other.topic), consumer(other.consumer), offset(other.offset),
      partition(other.partition) {}

int BrightnESS::FileWriter::Streamer::disconnect() {
  int return_code = consumer->stop(topic, partition.value());
  delete topic;
  delete consumer;
  return return_code;
}

int BrightnESS::FileWriter::Streamer::closeStream() {
  return consumer->stop(topic, partition.value());
}

int BrightnESS::FileWriter::Streamer::connect(const std::string &broker,
                                              const std::string &topic_name) {
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  std::string debug, errstr;
  conf->set("metadata.broker.list", broker, errstr);
  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
  }

  conf->set("fetch.message.max.bytes", "1000000000", errstr);
  conf->set("receive.message.max.bytes", "1000000000", errstr);

  if (topic_name.empty()) {
    throw std::runtime_error("Topic required");
  }

  if (!(consumer = RdKafka::Consumer::create(conf, errstr))) {
    throw std::runtime_error("Failed to create consumer: " + errstr);
  }

  if (!(topic = RdKafka::Topic::create(consumer, topic_name, tconf, errstr))) {
    throw std::runtime_error("Failed to create topic: " + errstr);
  }

  // Start consumer for topic+partition at start offset
  RdKafka::ErrorCode resp = consumer->start(topic, partition.value(), offset.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
  }
  return int(RdKafka::ERR_NO_ERROR);
}

BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::get_last_offset() {

  RdKafka::ErrorCode resp = consumer->stop(topic, partition.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
    return ProcessMessageResult::ERR();
  }
  resp = consumer->start(topic, partition.value(), RdKafka::Consumer::OffsetTail(1));
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
    return ProcessMessageResult::ERR();
  }
  RdKafka::Message *msg =
      consumer->consume(topic, partition.value(), consumer_timeout.count());
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    ProcessMessageResult::ERR();
  }
  last_offset = RdKafkaOffset(msg->offset());
  return ProcessMessageResult::OK();
}

/// Method specialisation for a functor with signature void f(void*). The
/// method applies f to the message payload.
template <>
BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::write(
    std::function<ProcessMessageResult(void *, int)> &f) {
  RdKafka::Message *msg =
      consumer->consume(topic, partition.value(), consumer_timeout.count());
  if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
    std::cout << "eof reached" << std::endl;
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
              << std::endl;
    return ProcessMessageResult::ERR();;
  }
  message_length = msg->len();
  last_offset = RdKafkaOffset(msg->offset());
  return f(msg->payload(), msg->len());
}

template <>
BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::write(
    BrightnESS::FileWriter::DemuxTopic &mp) {
  RdKafka::Message *msg =
      consumer->consume(topic, partition.value(), consumer_timeout.count());
  if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
    //    std::cout << "eof reached" << std::endl;
    return ProcessMessageResult::OK();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    //    std::cout << "Failed to consume message:
    //    "+RdKafka::err2str(msg->err()) << std::endl;
    return ProcessMessageResult::ERR(); // msg->err();
  }
  message_length = msg->len();
  last_offset = RdKafkaOffset(msg->offset());
  auto result = mp.process_message((char *)msg->payload(), msg->len());
  std::cout << "process_message:\t" << result.ts() << std::endl;
  return result;
}

/// Implements some algorithm in order to search in the kafka queue the first
/// message with timestamp >= the timestam of beginning of data taking
/// (assumed to be stored in Source)
template <>
BrightnESS::FileWriter::TimeDifferenceFromMessage_DT
BrightnESS::FileWriter::Streamer::jump_back<BrightnESS::FileWriter::DemuxTopic>(
    BrightnESS::FileWriter::DemuxTopic &td) {

  if (last_offset == RdKafkaOffset(0) ) {
    if (get_last_offset().is_ERR()) {
      return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
    }
  }

  RdKafka::ErrorCode resp = consumer->stop(topic, partition.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
  }

  if (last_offset.value() < 0)
    last_offset = RdKafkaOffset(0);
  last_offset -= RdKafkaOffset(step_back_amount);

  resp = consumer->start(topic, partition.value(), last_offset.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
  }

  RdKafka::Message *msg =
      consumer->consume(topic, partition.value(), consumer_timeout.count());
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
              << std::endl;
    return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
  }
  return td.time_difference_from_message((char *)msg->payload(), msg->len());
}

template <>
BrightnESS::FileWriter::TimeDifferenceFromMessage_DT
BrightnESS::FileWriter::Streamer::jump_back<std::function<
    BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void *, int)>>(
    std::function<
        BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void *, int)> &f) {

  if (last_offset.value() == 0) {
    // prima provare cmq a consumare un messaggio
    if (get_last_offset().is_ERR()) {
      return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
    }
  }

  RdKafka::ErrorCode resp = consumer->stop(topic, partition.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to stop consumer: " +
                             RdKafka::err2str(resp));
  }

  last_offset -= RdKafkaOffset(step_back_amount);
  if (last_offset.value() < 0)
    last_offset = RdKafkaOffset(0);

  resp = consumer->start(topic, partition.value(), last_offset.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
                             RdKafka::err2str(resp));
  }

  RdKafka::Message *msg =
      consumer->consume(topic, partition.value(), consumer_timeout.count());
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
              << std::endl;
    return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
  }
  return f((char *)msg->payload(), msg->len());
}

template <>
std::map<std::string, int64_t> &&
BrightnESS::FileWriter::Streamer::scan_timestamps<
    BrightnESS::FileWriter::DemuxTopic>(
    BrightnESS::FileWriter::DemuxTopic &demux) {

  std::map<std::string, int64_t> timestamp;
  for (auto &s : demux.sources()) {
    timestamp[s.source()] = -1;
  }
  int n_sources = demux.sources().size();

  do {
    RdKafka::Message *msg =
        consumer->consume(topic, partition.value(), consumer_timeout.count());
    if (msg->err() != RdKafka::ERR__PARTITION_EOF)
      break;
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
                << std::endl;
      continue;
    }
    DemuxTopic::DT t =
        demux.time_difference_from_message((char *)msg->payload(), msg->len());
    // HP: messages within the same source are ordered
    if (timestamp[t.sourcename] == -1) {
      n_sources--;
      timestamp[t.sourcename] = t.dt;
    }

  } while (n_sources < 1);

  return std::move(timestamp);
}
