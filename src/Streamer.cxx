#include <librdkafka/rdkafkacpp.h>
#include "logger.h"

#include "Streamer.hpp"
#include "helper.h"

/// TODO:
///   - reconnect if consumer return broker error
///   - search backward at connection setup

int64_t BrightnESS::FileWriter::Streamer::step_back_amount = 10;
milliseconds BrightnESS::FileWriter::Streamer::consumer_timeout = milliseconds(1000);

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
    if (conf->set("fetch.message.max.bytes", "1000000000", errstr) !=
	RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (conf->set("receive.message.max.bytes", "1000000000", errstr) !=
	RdKafka::Conf::CONF_OK) {
      throw std::runtime_error("Failed to initialise configuration: " + errstr);
    }
    if (!debug.empty()) {
      if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
	throw std::runtime_error("Failed to initialise configuration: " + errstr);
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
  RdKafka::ErrorCode resp = _consumer->start(_topic,
					     _partition.value(),
					     _offset.value());
  if (resp != RdKafka::ERR_NO_ERROR) {
    throw std::runtime_error("Failed to start consumer: " +
			     RdKafka::err2str(resp));
  }
  int64_t high,low;
  _consumer->query_watermark_offsets(topic_name,_partition.value(),&low,&high,1000);
  std::cout << "\nlow = " << low << "\t" << "high = " << high << "\n\n";
  _begin_offset = RdKafkaOffset(low);
  if(_offset.value() == RdKafka::Topic::OFFSET_END)
    _offset = RdKafkaOffset(high);
  _last_visited_offset = _offset;
}

BrightnESS::FileWriter::Streamer::Streamer(const Streamer &other)
  : _topic(other._topic), _consumer(other._consumer), 
    _offset(other._offset), _begin_offset(other._begin_offset),
    _partition(other._partition) {}

int BrightnESS::FileWriter::Streamer::disconnect() {
  int return_code = _consumer->stop(_topic, _partition.value());
  delete _topic;
  delete _consumer;
  return return_code;
}

int BrightnESS::FileWriter::Streamer::closeStream() {
  return _consumer->stop(_topic, _partition.value());
}

int BrightnESS::FileWriter::Streamer::connect(const std::string &broker,
                                              const std::string &topic_name,
					      const RdKafkaOffset &offset,
					      const RdKafkaPartition &partition) {
  (*this) = std::move(Streamer(broker,topic_name,offset,partition));
  return int(RdKafka::ERR_NO_ERROR);
}

BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::get_offset() {
  // if( _offset.value() == 0 ) {
  _consumer->seek(_topic,_partition.value(),RdKafka::Consumer::OffsetTail(1),100);
  RdKafka::Message *msg = _consumer->consume(_topic, _partition.value(), 100);
  if( msg->err() != RdKafka::ERR_NO_ERROR) {
    _offset = RdKafkaOffset(RdKafka::Topic::OFFSET_INVALID);
    return ProcessMessageResult::ERR();
  }
  _offset = RdKafkaOffset(msg->offset());
  // }
  return ProcessMessageResult::OK();
}


int BrightnESS::FileWriter::Streamer::jump_back_impl(const int& amount) {
  int reach_beginning=0;
  _last_visited_offset = _offset;
  int position = _offset.value()-(amount+1); // pone message is consumed reading the offet
  if( position < _begin_offset.value() ) {
    position = _begin_offset.value();
    reach_beginning = 1;
  }
  auto err = _consumer->seek(_topic,
			    _partition.value(),
			    position,
			    1000);
  if( err != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to seek :\t" << RdKafka::err2str(err) << std::endl;
    reach_beginning = -1;
  }
  _offset = RdKafkaOffset(position);
  return reach_beginning;
}


template <>
BrightnESS::FileWriter::TimeDifferenceFromMessage_DT
BrightnESS::FileWriter::Streamer::jump_back<BrightnESS::FileWriter::DemuxTopic>(
										BrightnESS::FileWriter::DemuxTopic &td, const int amount) {

  if (_offset.value() == 0 ) {
    if (get_offset().is_ERR()) {
      return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
    }
  }
  int is_beginning = jump_back_impl(amount);
  if(is_beginning < 0) {
    TimeDifferenceFromMessage_DT::ERR();
  }
  RdKafka::Message *msg =
    _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
  if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
    std::cout << "eof reached" << std::endl;
    return TimeDifferenceFromMessage_DT::ERR();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
              << std::endl;
    return TimeDifferenceFromMessage_DT::ERR();
  }

  return td.time_difference_from_message((char *)msg->payload(), msg->len());
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
    return ProcessMessageResult::ERR();
  }
  message_length = msg->len();
  _offset = RdKafkaOffset(msg->offset());

  auto result = mp.process_message((char *)msg->payload(), msg->len());
  //  std::cout << "process_message:\t" << result.ts() << std::endl;
  return result;
}


template <>
std::map<std::string, int64_t>&
BrightnESS::FileWriter::Streamer::scan_timestamps<>(BrightnESS::FileWriter::DemuxTopic &mp,
						    std::map<std::string, int64_t>& ts_list) {

  do {
    RdKafka::Message *msg =
      _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
                << std::endl;
      break;
    }
    _offset = RdKafkaOffset(msg->offset());
    DemuxTopic::DT t = mp.time_difference_from_message((char *)msg->payload(), msg->len());
    if( !ts_list[t.sourcename] )
      ts_list[t.sourcename]=t.dt;
    else {
      if ( t.dt < ts_list[t.sourcename] ) {
	ts_list[t.sourcename]=t.dt;
      }
    }
  } while ( (_offset != _last_visited_offset)
	    && (ts_list.size() != 1)
	    );

  return ts_list;
}


template<>
std::map<std::string,int64_t> 
BrightnESS::FileWriter::Streamer::get_initial_time<>(BrightnESS::FileWriter::DemuxTopic &mp, 
						     const ESSTimeStamp tp) {
  std::map<std::string,int64_t> m;
  bool stop = false;
  do {
    jump_back(mp,step_back_amount);
    scan_timestamps(mp,m);
    bool cont = true;
    for(auto & v: m) {
      cont &= (v.second > tp.count());
    }
    stop = (m.size() >= 1) && cont;
  } while( stop );
  
  return std::move(m);
} 



template <>
BrightnESS::FileWriter::TimeDifferenceFromMessage_DT
BrightnESS::FileWriter::Streamer::jump_back<std::function<
  BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void *, int)>>(
								      std::function<
								      BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void *, int)> &f,const int amount) {
  if (_offset.value() == 0) {
    if (get_offset().is_ERR()) {
      return BrightnESS::FileWriter::TimeDifferenceFromMessage_DT::ERR();
    }
  }
  int is_beginning = jump_back_impl(amount);
  if(is_beginning < 0) {
    TimeDifferenceFromMessage_DT::ERR();
  }
  RdKafka::Message *msg =
    _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
  if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
    std::cout << "eof reached" << std::endl;
    return TimeDifferenceFromMessage_DT::ERR();
  }
  if (msg->err() != RdKafka::ERR_NO_ERROR) {
    std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
              << std::endl;
    return TimeDifferenceFromMessage_DT::ERR();
  }
  return f((char *)msg->payload(), msg->len());
}

template <>
BrightnESS::FileWriter::ProcessMessageResult
BrightnESS::FileWriter::Streamer::write<>(
    std::function<ProcessMessageResult(void *, int)> &f) {
  RdKafka::Message *msg =
      _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
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
  _offset = RdKafkaOffset(msg->offset());
  std::cout << "msg->offset() = " << msg->offset() << "\n";  
  return f(msg->payload(), msg->len());
}


template <>
std::map<std::string, int64_t>&
BrightnESS::FileWriter::Streamer::scan_timestamps<>(std::function<
						    BrightnESS::FileWriter::TimeDifferenceFromMessage_DT(void *, int)> &f, std::map<std::string, int64_t>& ts_list) {

  do {
    RdKafka::Message *msg =
      _consumer->consume(_topic, _partition.value(), consumer_timeout.count());
    if (msg->err() != RdKafka::ERR_NO_ERROR) {
      std::cout << "Failed to consume message: " + RdKafka::err2str(msg->err())
                << std::endl;
      break;
    }
    _offset = RdKafkaOffset(msg->offset());
    DemuxTopic::DT t = f((char *)msg->payload(), msg->len());
    if( !ts_list[t.sourcename] )
      ts_list[t.sourcename]=t.dt;
    else {
      if ( t.dt < ts_list[t.sourcename] ) {
	ts_list[t.sourcename]=t.dt;
      }
    }
  } while ( (_offset != _last_visited_offset)
	    && (ts_list.size() != 1)
	    );
  return ts_list;
}

template<>
std::map<std::string,int64_t> 
BrightnESS::FileWriter::Streamer::get_initial_time<>(std::function<TimeDifferenceFromMessage_DT(void*,int)>& f, const ESSTimeStamp tp) {
  std::map<std::string,int64_t> m;

  bool stop = false;
  do {
    jump_back(f,step_back_amount);
    scan_timestamps(f,m);
    bool cont = true;
    for(auto & v: m) {
      cont &= (v.second > tp.count());
    }
    stop = (m.size() >= 1) && cont;
  } while( stop );
  
  return std::move(m);
} 


