#pragma once

#include <iostream>
#include <string>
#include <functional>

//#include "ScopeGuard.hpp"

// forward definitions
namespace RdKafka {
  class Topic;
  class Consumer;
}


// actually a "kafka streamer"
class Streamer {
public:
  Streamer() : offset(0) { };
  Streamer(const std::string&, const std::string&);
  Streamer(const Streamer&);
  
  //  ~Streamer(); // disconnect

  /// Receives from stream and apply callback. If non specialised method is used
  /// writes a message and return -1, doing nothing. For specific usage
  /// implement specialised version.
  template<class T>
  bool recv(T& f) { message_length=0; std::cout << "fake_recv\n"; return false; }

  template<class T>
  bool search_backward(T& f) { message_length=0; std::cout << "fake_search\n"; return false; }
  
  int connect(const std::string&, const std::string&);
  int disconnect();
  
  /// Returns message length
  size_t len() { return message_length; }
  
private:
  RdKafka::Topic *topic;
  RdKafka::Consumer *consumer;
  int32_t partition = 0;
  uint64_t offset;
  size_t message_length;

  /// Actual implementation of the receive methods.
  template<class T>
  bool recv_impl(T& f, void*) { };
};


template<> bool Streamer::recv<std::function<void(void*)> >(std::function<void(void*)>&);
template<> bool Streamer::recv_impl<std::function<void(void*)> >(std::function<void(void*)>&, void*);

template<> bool Streamer::search_backward<std::function<void(void*)> >(std::function<void(void*)>&);
