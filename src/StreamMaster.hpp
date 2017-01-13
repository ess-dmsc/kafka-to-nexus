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
//struct Demux;
//using Demux=Pizza;

constexpr std::chrono::milliseconds operator "" _ms(const unsigned long long int value) {
  return std::chrono::milliseconds(value);
}




template<typename Streamer, typename Demux>
struct StreamMaster {
  
  StreamMaster() { };
  
  /// FileWriterCommand acts as a factory and populates the source list
  StreamMaster(Pino&) {
    std::vector<std::string> pizza_list={"Hawaiian","Hawaiian","Deluxe","Hawaiian"};
    for( auto& p: pizza_list)
      demux.push_back(Pino::createPizza(p));
    for( auto& p : demux )
      std::cout << "Price of " << p->getName() << " is " << p->getPrice() << std::endl;
  }; 

  StreamMaster(FileWriterCommand&) { };
  
  void push_back(const std::string& server, const std::string& topic);

  template <class Fun>
  void run(Fun& f) {
    // auto loop = std::thread( [&] { this->loop_streamer(); } );
    // loop.join()
  }
  
private:
  static std::chrono::milliseconds duration;
  std::map<std::string,Streamer> streamer;
  std::vector< std::unique_ptr<Demux> > demux;  
  std::atomic<int> index;
  std::map< std::string,std::vector< std::pair<std::string,int64_t> > > timestamp_list;
};

template<typename S,typename D>
std::chrono::milliseconds StreamMaster<S,D>::duration=10_ms;
