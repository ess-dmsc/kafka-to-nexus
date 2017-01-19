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

#include "DemuxTopic.h"

static const int Mega=1024*1024;
static int accum;
static int count;
std::function<void(void*,int)> process_data = [](void* data, int size) {
  std::cout << std::string((char*)data) << "\t" << size << std::endl;
  accum+=size;
  if(accum > (Mega)) {
    accum -= Mega;
    std::cout << (++count) << " MB\n";
  }
};

struct Streamer;
struct FileWriterCommand;


constexpr std::chrono::milliseconds operator "" _ms(const unsigned long long int value) {
  return std::chrono::milliseconds(value);
}

using namespace BrightnESS::FileWriter;


template<typename Streamer, typename Demux>
struct StreamMaster {
  
  StreamMaster() { };
  
  StreamMaster(std::string& broker, std::vector<std::string> topic) {
    for( auto& t: topic) {
      demux.push_back(Demux(t));
      streamer[t] = Streamer(broker,t);
    }
  };

  StreamMaster(std::string& broker, FileWriterCommand&) { };
  
  void push_back(const std::string& broker, const std::string& topic) {
    demux.push_back(Demux(topic));
    streamer[topic] = Streamer(broker,topic);
  }

  void start() {
    keep_writing = true;
    auto loop = std::thread( [&] { this->run(); } );
    loop.join();
  }

  void stop() {
    keep_writing = false;
  }


  void run() {
    while( keep_writing ) {
      for(index=0;index<demux.size();++index) {
        run_on_demux(demux[index]);
      }
    }
  }

  
  int run_on_demux(Demux& dem) {
    start_source_time = std::chrono::system_clock::now();
    int value;

    do {
      streamer[dem.topic()].write(dem.process_data);
    } while( value == 0 && ((std::chrono::system_clock::now() - start_source_time) < duration) );
    return value;
    //    write_source(streamer[dem.topic()]);
  }
  
  
  // int write_source(Streamer& strm// , Source& s
  //                  )  {
  //   start_source_time = std::chrono::system_clock::now();
  //   int value;
  //   do {
  //     value = strm.write(src.process_data);
  //   } while( value == 0 && ((std::chrono::system_clock::now() - start_source_time) > duration) );
  //   return value;
  // }


    
private:
  static std::chrono::milliseconds duration;
  std::chrono::system_clock::time_point start_source_time;
  std::map<std::string,Streamer> streamer;
  std::vector<Demux> demux;  
  std::atomic<int> index;
  std::map< std::string,std::vector< std::pair<std::string,int64_t> > > timestamp_list;
  bool keep_writing=false;
};

template<typename S,typename D>
std::chrono::milliseconds StreamMaster<S,D>::duration=1_ms;
