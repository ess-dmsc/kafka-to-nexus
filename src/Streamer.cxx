#include "Streamer.hpp"
#include <librdkafka/rdkafkacpp.h>
// #include "KafkaMock.hpp"


/// TODO:
///   - reconnect if consumer return broker error
///   - search backward at connection setup


int64_t BrightnESS::FileWriter::Streamer::step_back_amount = 1000;

BrightnESS::FileWriter::Streamer::Streamer(const std::string& broker, const std::string& topic_name, const int64_t& p) : offset(RdKafka::Topic::OFFSET_END), partition(p) {

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf  = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  
  std::string debug,errstr;
  if (conf->set("metadata.broker.list", broker, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::runtime_error("Failed to initialise configuration: "+errstr);
  }
  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: "+errstr);
    }
  }

  conf->set("fetch.message.max.bytes", "1000000000", errstr);
  conf->set("receive.message.max.bytes", "1000000000", errstr);

  if(topic_name.empty()) {
    throw std::runtime_error("Topic required");
  }
  
  if ( !(consumer = RdKafka::Consumer::create(conf, errstr)) ) {
    throw std::runtime_error("Failed to create consumer: "+errstr);
  }

  if ( !(topic = RdKafka::Topic::create(consumer, topic_name,tconf, errstr)) ) {
    throw std::runtime_error("Failed to create topic: "+errstr);
  }
  
  // Start consumer for topic+partition at start offset
  RdKafka::ErrorCode resp = consumer->start(topic, partition, offset);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
  }

}


BrightnESS::FileWriter::Streamer::Streamer(const Streamer& other) : topic(other.topic), consumer(other.consumer), offset(other.offset), partition(other.partition) { }

int BrightnESS::FileWriter::Streamer::disconnect() {
  int return_code = consumer->stop(topic,partition);
  delete topic;
  delete consumer;
  return return_code;
}


int BrightnESS::FileWriter::Streamer::closeStream() { return consumer->stop(topic,partition); }

int BrightnESS::FileWriter::Streamer::connect(const std::string& broker,
                      const std::string& topic_name) {
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf  = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  std::string debug,errstr;
  conf->set("metadata.broker.list", broker, errstr);
  if (!debug.empty()) {
    if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: "+errstr);
    }
  }
  
  conf->set("fetch.message.max.bytes", "1000000000", errstr);
  conf->set("receive.message.max.bytes", "1000000000", errstr);
  
  if(topic_name.empty()) {
    throw std::runtime_error("Topic required");
  }
  
  if ( !(consumer = RdKafka::Consumer::create(conf, errstr)) ) {
    throw std::runtime_error("Failed to create consumer: "+errstr);
  }
  
  if ( !(topic = RdKafka::Topic::create(consumer, topic_name,tconf, errstr)) ) {
    throw std::runtime_error("Failed to create topic: "+errstr);
  }
  
  // Start consumer for topic+partition at start offset
  RdKafka::ErrorCode resp = consumer->start(topic, partition, offset);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
  }
  return int(RdKafka::ERR_NO_ERROR);
}


BrightnESS::FileWriter::ProcessMessageResult BrightnESS::FileWriter::Streamer::process_last_message() {
  
  RdKafka::ErrorCode resp = consumer->stop(topic,partition);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
    return ProcessMessageResult::ERR();
  }
  resp = consumer->start(topic, partition, RdKafka::Consumer::OffsetTail(1));
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
    return ProcessMessageResult::ERR();
  }
  RdKafka::Message *msg = consumer->consume(topic, partition, consumer_timeout.count());
  if(msg->err() != RdKafka::ERR_NO_ERROR) {
    ProcessMessageResult::ERR();
  }

  last_offset = msg->offset();
  return ProcessMessageResult::OK();
}




/// Method specialisation for a functor with signature void f(void*). The
/// method applies f to the message payload.
template<>
BrightnESS::FileWriter::ProcessMessageResult BrightnESS::FileWriter::Streamer::write(std::function<ProcessMessageResult(void*,int)>& f) {
  RdKafka::Message *msg = consumer->consume(topic, partition, consumer_timeout.count());
  if( msg->err() == RdKafka::ERR__PARTITION_EOF) {
    std::cout << "eof reached" << std::endl;
    return ProcessMessageResult::OK();
  }
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: "+RdKafka::err2str(msg->err()) << std::endl;
    return ProcessMessageResult::ERR();//msg->err();
  }
  message_length = msg->len();
  last_offset = msg->offset();
  return f(msg->payload(),msg->len());
}

template<>
BrightnESS::FileWriter::ProcessMessageResult BrightnESS::FileWriter::Streamer::write(BrightnESS::FileWriter::DemuxTopic & mp) {
  RdKafka::Message *msg = consumer->consume(topic, partition, consumer_timeout.count());
  if( msg->err() == RdKafka::ERR__PARTITION_EOF) {
    //    std::cout << "eof reached" << std::endl;
    return ProcessMessageResult::OK();
  }
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    //    std::cout << "Failed to consume message: "+RdKafka::err2str(msg->err()) << std::endl;
    return ProcessMessageResult::ERR();//msg->err();
  }
  message_length = msg->len();
  last_offset = msg->offset();
  return mp.process_message((char*)msg->payload(),msg->len());
}


/// Implements some algorithm in order to search in the kafka queue the first
/// message with timestamp >= the timestam of beginning of data taking 
/// (assumed to be stored in Source)
template<>
BrightnESS::FileWriter::TimeDifferenceFromMessage_DT BrightnESS::FileWriter::Streamer::search_backward<BrightnESS::FileWriter::DemuxTopic>(BrightnESS::FileWriter::DemuxTopic& td) {

  if(last_offset == 0 ) {
    if( process_last_message().is_ERR() ) {
      return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
    }
  }
  
  RdKafka::ErrorCode resp = consumer->stop(topic,partition);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
  }

  if(last_offset < 0) last_offset=0;
  last_offset -= step_back_amount;

  resp = consumer->start(topic, partition, last_offset);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
  }
  
  RdKafka::Message *msg = consumer->consume(topic, partition, consumer_timeout.count());
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: "+RdKafka::err2str(msg->err()) << std::endl;
    return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
  }
  return td.time_difference_from_message((char*)msg->payload(),msg->len());
}


template<>
BrightnESS::FileWriter::TimeDifferenceFromMessage_DT BrightnESS::FileWriter::Streamer::search_backward<std::function<BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void*,int)> >(std::function<BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void*,int)>& f) {

  if(last_offset == 0 ) {
    if( process_last_message().is_ERR() ) {
      return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
    }
  }
  
  RdKafka::ErrorCode resp = consumer->stop(topic,partition);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to stop consumer: "+RdKafka::err2str(resp));
  }
  
  last_offset -= step_back_amount;
  if(last_offset < 0) last_offset=0;

  resp = consumer->start(topic, partition, last_offset);
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: "+RdKafka::err2str(resp));
  }
  
  RdKafka::Message *msg = consumer->consume(topic, partition, consumer_timeout.count());
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: "+RdKafka::err2str(msg->err()) << std::endl;
    return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
  }
  return f((char*)msg->payload(),msg->len());
}


  
milliseconds BrightnESS::FileWriter::Streamer::consumer_timeout = 1000_ms;
