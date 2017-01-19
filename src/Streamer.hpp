#pragma once

#include <iostream>
#include <string>
#include <functional>

// forward definitions
namespace RdKafka {
  class Topic;
  class Consumer;
}

// actually a "kafka streamer"
struct Streamer {
public:
  Streamer() : offset(0), partition(0) { };
  Streamer(const std::string&, const std::string&, const int64_t& p=0);
  Streamer(const Streamer&);
  
  //  ~Streamer(); // disconnect

  /// Receives from stream and apply callback. If non specialised method is used
  /// writes a message and return -1, doing nothing. For specific usage
  /// implement specialised version.
  template<class T>
  int write(T& f) { message_length=0; std::cout << "fake_recv\n"; return -1; }

  template<class T>
  bool search_backward(T& f, int m=1) { message_length=0; std::cout << "fake_search\n"; return false; }
  
  int connect(const std::string&, const std::string&);
  int disconnect();
  
  /// Returns message length
  size_t len() { return message_length; }

  static int64_t backward_offset;
private:
  RdKafka::Topic *topic;
  RdKafka::Consumer *consumer;
  uint64_t offset;
  int32_t partition = 0;
  size_t message_length;
};

template<> int Streamer::write<std::function<void(void*,int)> >(std::function<void(void*,int)>&);

template<> bool Streamer::search_backward<std::function<void(void*)> >(std::function<void(void*)>&, int);
