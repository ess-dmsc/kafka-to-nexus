#pragma once

#include <iostream>
#include <string>
#include <functional>
#include "DemuxTopic.h"

// forward definitions
namespace RdKafka {
  class Topic;
  class Consumer;
}

namespace BrightnESS {
  namespace FileWriter {


    
// actually a "kafka streamer"
struct Streamer {
  static const int consumer_timeout = 10;
  static int64_t step_back_amount;

  Streamer() : offset(0), partition(0) { };
  Streamer(const std::string&, const std::string&, const int64_t& p=0);
  Streamer(const Streamer&);
  
  //  ~Streamer(); // disconnect

  /// Receives from stream and apply callback. If non specialised method is used
  /// writes a message and return -1, doing nothing. For specific usage
  /// implement specialised version.
  template<class T>
  ProcessMessageResult write(T& f) {
    message_length=0;
    std::cout << "fake_recv\n";
    return ProcessMessageResult();
  }

  template<class T>
  TimeDifferenceFromMessage_DT search_backward(T& f) {
    message_length=0;
    std::cout << "fake_search\n";
    return TimeDifferenceFromMessage_DT("",0);
  }
  
  int connect(const std::string&, const std::string&);
  int disconnect();
  int closeStream();
  
  /// Returns message length
  size_t len() { return message_length; }

private:
  RdKafka::Topic *topic;
  RdKafka::Consumer *consumer;
  uint64_t offset;
  int64_t last_offset=0;
  int64_t step_back_offset=0;
  int32_t partition = 0;
  size_t message_length;

};

template<> ProcessMessageResult Streamer::write<std::function<ProcessMessageResult(void*,int)> >(std::function<ProcessMessageResult(void*,int)>&);
template<> ProcessMessageResult Streamer::write<BrightnESS::FileWriter::DemuxTopic>(BrightnESS::FileWriter::DemuxTopic &);

template<> TimeDifferenceFromMessage_DT Streamer::search_backward<BrightnESS::FileWriter::DemuxTopic>(BrightnESS::FileWriter::DemuxTopic&);

  }
}
