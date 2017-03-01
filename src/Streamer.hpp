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
}

namespace BrightnESS {
  namespace FileWriter {
    
// actually a "kafka streamer"
struct Streamer {
  static milliseconds consumer_timeout;
  static int64_t step_back_amount;
  

  Streamer() : offset(0), partition(0) { };
  Streamer(const std::string&, const std::string&, const RdKafkaOffset& =RdKafkaOffset(-1),const RdKafkaPartition& =RdKafkaPartition(0));
  Streamer(const Streamer&);
  
  //  ~Streamer(); // disconnect

  /// Receives from stream and apply callback. If non specialised method is used
  /// writes a message and return -1, doing nothing. For specific usage
  /// implement specialised version.
  template<class T>
  ProcessMessageResult write(T& f) {
    message_length=0;
    std::cout << "fake_recv\n";
    return ProcessMessageResult::ERR();
  }

  int connect(const std::string&, const std::string&);
  int disconnect();
  int closeStream();
  
  /// Returns message length
  size_t len() { return message_length; }
  
  ProcessMessageResult get_last_offset();
  
  template<class T>
  std::map<std::string,int64_t>&& scan_timestamps(T&);
  
// make PRIVATE
  template<class T>
  TimeDifferenceFromMessage_DT jump_back(T& f) {
    message_length=0;
    std::cout << "fake_search\n";
    return TimeDifferenceFromMessage_DT::ERR();
  }

  template<class T>
  std::map<std::string,int64_t> search_backward(T& demux, const ESSTimeStamp t0) {
    jump_back(demux);
    auto ts = scan_timestamp(demux);
    if(/* any ts < 0 || > ESSTimestamp? */0)
      return search_backward(demux,t0);
    return ts;
  }

  
private:
  RdKafka::Topic *topic;
  RdKafka::Consumer *consumer;
  RdKafkaOffset offset;
  RdKafkaOffset last_offset=RdKafkaOffset(-1);
  int64_t step_back_offset=0;
  RdKafkaPartition partition;
  size_t message_length;

};

    template<> ProcessMessageResult Streamer::write<std::function<ProcessMessageResult(void*,int)> >(std::function<ProcessMessageResult(void*,int)>&);
    template<> ProcessMessageResult Streamer::write<BrightnESS::FileWriter::DemuxTopic>(BrightnESS::FileWriter::DemuxTopic &);
    
    template<> TimeDifferenceFromMessage_DT Streamer::jump_back<BrightnESS::FileWriter::DemuxTopic>(BrightnESS::FileWriter::DemuxTopic&);
    template<> TimeDifferenceFromMessage_DT Streamer::jump_back<std::function<TimeDifferenceFromMessage_DT(void*,int)> >(std::function<TimeDifferenceFromMessage_DT(void*,int)>&);

    template<> std::map<std::string,int64_t>&& Streamer::scan_timestamps<BrightnESS::FileWriter::DemuxTopic>(BrightnESS::FileWriter::DemuxTopic &);

  }
}

