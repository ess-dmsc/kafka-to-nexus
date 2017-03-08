#pragma once

#include <iostream>
#include <string>
#include <functional>
#include <map>

#include "DemuxTopic.h"
#include "utils.h"

// forward definitions
namespace RdKafka {
  class Topic;
  class Consumer;
  class TopicPartition;
  class Message;
}

namespace BrightnESS {
  namespace FileWriter {
    
// actually a "kafka streamer"
struct Streamer {
  static milliseconds consumer_timeout;
  static int64_t step_back_amount;
  

  Streamer() { };
  Streamer(const std::string&, const std::string&, 
	   const RdKafkaOffset& = RdKafkaOffsetEnd, 
	   const RdKafkaPartition& = RdKafkaPartition(0));
  Streamer(const Streamer&);
  
  //  ~Streamer(); // disconnect
  template<class T>
  ProcessMessageResult write(T& f) {
    message_length=0;
    std::cout << "fake_recv\n";
    return ProcessMessageResult::ERR();
  }

  int connect(const std::string&, const std::string&, 
	   const RdKafkaOffset& = RdKafkaOffsetEnd, 
	   const RdKafkaPartition& = RdKafkaPartition(0));
  int disconnect();
  int closeStream();
  
  /// Returns message length
  size_t len() { return message_length; }
  
  ProcessMessageResult get_offset();

  template<class T>
  std::map<std::string,int64_t> get_initial_time(T& x, const ESSTimeStamp tp) {
    std::cout << "no initial timepoint\n";
    return std::map<std::string,int64_t>();
  }

  
  template<class T>
  std::map<std::string,int64_t>& scan_timestamps(T& x, std::map<std::string,int64_t>& m) {
    std::cout << "no scan\n";
    return m;
  }
  
// make PRIVATE
  template<class T>
  TimeDifferenceFromMessage_DT jump_back(T& x,const int=100) {
    message_length=0;
    std::cout << "no search\n";
    return TimeDifferenceFromMessage_DT::ERR();
  }


private:
  RdKafka::Topic *_topic;
  RdKafka::Consumer *_consumer;
  RdKafka::TopicPartition *_tp;
  RdKafkaOffset _offset;
  RdKafkaOffset _begin_offset;
  RdKafkaOffset _last_visited_offset;
  int64_t step_back_offset;
  RdKafkaPartition _partition;
  size_t message_length;

  int jump_back_impl(const int&);
};

    template<> ProcessMessageResult Streamer::write<>(std::function<ProcessMessageResult(void*,int)>&);
    template<> ProcessMessageResult Streamer::write<>(BrightnESS::FileWriter::DemuxTopic &);
    
    template<> TimeDifferenceFromMessage_DT Streamer::jump_back<>(BrightnESS::FileWriter::DemuxTopic&,const int);
    template<> TimeDifferenceFromMessage_DT Streamer::jump_back<>(std::function<TimeDifferenceFromMessage_DT(void*,int)>&,const int);

    template<> std::map<std::string,int64_t>& Streamer::scan_timestamps<>(BrightnESS::FileWriter::DemuxTopic &, std::map<std::string,int64_t>&);
    template<> std::map<std::string,int64_t>& Streamer::scan_timestamps<>(std::function<TimeDifferenceFromMessage_DT(void*,int)>&, std::map<std::string,int64_t>&);

    template<> std::map<std::string,int64_t> Streamer::get_initial_time<>(std::function<TimeDifferenceFromMessage_DT(void*,int)>&, const ESSTimeStamp);
    template<> std::map<std::string,int64_t> Streamer::get_initial_time<>(BrightnESS::FileWriter::DemuxTopic &, const ESSTimeStamp);
    
    
  }
}

