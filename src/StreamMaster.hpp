#pragma once

#include <iostream>
#include <string>
#include <functional>
#include <memory>
#include <vector>
#include <map>

#include <chrono>
#include <thread>
#include <atomic>


struct Source {
  Source(const std::string& t, const std::string& s) : topic(t), source(s) { };
  Source(const Source& other) : topic(other.topic),  source(other.source) { };
  std::string topic,source;
};


template<typename S>
struct StreamMaster {

  StreamMaster()  : index(0), duration(10), is_ready(false) { }

  void push_back(const std::string& server, const std::string& topic, const std::string& src) {
     
    source.push_back(Source(topic,src));
    if( streamer.find(topic) == streamer.end() ) {
      streamer[topic] = S(server,topic);
    }
    is_ready=true;
  }

  template <class Fun>
  void run(Fun& f) {
    if(!is_ready) {
      std::runtime_error("Streamer array not allocated.");
      // log

    }
    run_status=true;
    auto loop = std::thread( [&] { this->loop_streamer(); } );
    auto r = std::thread( [&]{ this->run_streamer(f); } );
    

    r.join();
    loop.join();
  }
  
  void set_duration(const std::chrono::milliseconds& t) {
    duration=t;
  }
  bool status() const {
    return is_ready;
  }

  
private:
  std::chrono::milliseconds duration;

  std::map<std::string,S> streamer;
  std::vector<Source> source;
  
  std::atomic<int> index;
  bool run_status;
  bool is_ready;

  template<class Fun>
  void run_streamer(Fun& f) {
    int i=1;
    while(run_status) {
      streamer[source[index].topic]. template recv<Fun>(f);
    }
  }

  
  void loop_streamer() {
    while(run_status) {
      std::this_thread::sleep_for(duration);
      index++;
      index = (index % source.size());
    }
  }

  
};
