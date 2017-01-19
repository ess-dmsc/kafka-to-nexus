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

#include "pizza_factory.hpp"

struct Streamer;
struct FileWriterCommand;

using Source = Ingredients;

constexpr std::chrono::milliseconds operator "" _ms(const unsigned long long int value) {
  return std::chrono::milliseconds(value);
}




template<typename Streamer, typename Demux>
struct StreamMaster {
  
  StreamMaster() { };
  
  /// FileWriterCommand acts as a factory and populates the source list
  StreamMaster(std::string& broker, Pino&) : index(0) {
    std::vector<std::string> pizza_list={"Hawaiian","Deluxe","HamAndMushroom"};
    for( auto& p: pizza_list) demux.push_back(Pino::createPizza(p));
    for(int i=0;i<demux.size();++i) {
      streamer[demux[i]->getName()] = Streamer(broker,demux[i]->getName());
    }
  };

  StreamMaster(std::string& broker, FileWriterCommand&) { };
  
  void push_back(const std::string& server, const std::string& topic);

  void start() {
    keep_writing = true;
    std::cout << "auto loop = std::thread( [&] { this->run(); } );\n";
    auto loop = std::thread( [&] { this->run(); } );
    loop.join();
  }

  void stop() {
    keep_writing = false;
  }


  void run() {
    while( keep_writing ) {
      for(index=0;index<demux.size();++index) {
        run_on_demux(*demux[index]);
      }
    }
    
  }
  void run_on_demux(Demux& dem) {
    //    std::cout << dem.getName() << ":"  << std::endl;
    write_source(streamer[dem.getName()], dem.s);
  }
  
  
  int write_source(Streamer& strm, Source& src)  {
    start_source_time = std::chrono::system_clock::now();
    int value;
    do {
      value = strm.write(src.process_data);
    } while( value == 0 && ((std::chrono::system_clock::now() - start_source_time) > duration) );
    return value;
  }


  
  void loop_streamer() {
    while( keep_writing ) {

      for( auto& d : demux ) {
        //        for( auto& s : d.getSource() ) {
          
          std::chrono::system_clock::time_point start_source_time = std::chrono::system_clock::now();
          while( streamer[d->getName()].write(d->s.process_data) == 0 && keep_writing ) {
            //            std::cout << d->getName() << std::endl;
            if( std::chrono::system_clock::now() - start_source_time > duration)
              break;
          }
          
          //        }
      }

    }

  }
  
private:
  static std::chrono::milliseconds duration;
  std::chrono::system_clock::time_point start_source_time;
  std::map<std::string,Streamer> streamer;
  std::vector< std::unique_ptr<Demux> > demux;  
  std::atomic<int> index;
  std::map< std::string,std::vector< std::pair<std::string,int64_t> > > timestamp_list;
  bool keep_writing=false;
};

template<typename S,typename D>
std::chrono::milliseconds StreamMaster<S,D>::duration=10_ms;
