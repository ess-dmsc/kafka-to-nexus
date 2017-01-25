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
#include <algorithm>

#include "FileWriterTask.h"
#include "DemuxTopic.h"

struct Streamer;
struct FileWriterCommand;

typedef int ESSTimeStamp;

constexpr std::chrono::milliseconds operator "" _ms(const unsigned long long int value) {
  return std::chrono::milliseconds(value);
}

using namespace BrightnESS::FileWriter;

template<typename Streamer, typename Demux>
struct StreamMaster {
  
  StreamMaster() : do_write(false), _stop(false) { };
  
  StreamMaster(std::string& broker, std::vector<Demux>& _demux) : demux(_demux), do_write(false), _stop(false) {
    for( auto& d: demux) {
      streamer[d.topic()] = Streamer(broker,d.topic());
    }
    loop = std::thread( [&] { this->run(); } );
  };

  StreamMaster(std::string& broker, std::unique_ptr<FileWriterTask> file_writer_task) :
    demux(file_writer_task->demuxers()), file_writer_task_(std::move(file_writer_task)), do_write(false), _stop(false) {
    for( auto& d: demux) {
      streamer[d.topic()] = Streamer(broker,d.topic());
    }
    loop = std::thread( [&] { this->run(); } );
    //   return loop.joinable();
  };

  ~StreamMaster() {
    _stop=true;
    if( loop.joinable() )
      loop.join();
  }

  
  bool start() {
    std::cout << "start()\n";
    // std::this_thread::sleep_for(1000_ms);
    do_write = true;
    return loop.joinable();
  }

  std::vector< std::pair<std::string,int> > stop() {
    std::cout << "stop()\n" << std::flush;
    std::this_thread::sleep_for(1000_ms);
    do_write = false;
    _stop = true;
    if( loop.joinable() ) loop.join();
    std::vector< std::pair<std::string,int> > stream_status;
    for (auto it=streamer.begin(); it!=streamer.end(); ++it) {
      stream_status.push_back(std::pair<std::string,int>(it->first,it->second.closeStream()));
    }
    return stream_status;
  }
  std::vector< std::pair<std::string,int> > stop(const ESSTimeStamp ts) {
    // verifica del tempo
    do_write = false;
    _stop = true;
    loop.join();
    std::vector< std::pair<std::string,int> > stream_status;
    for (auto it=streamer.begin(); it!=streamer.end(); ++it) {
      stream_status.push_back(std::pair<std::string,int>(it->first,it->second.closeStream()));
    }
    return stream_status;
  }

  bool poll_n_messages(const int n) { }

  
private:
  
  void run() {
    std::chrono::system_clock::time_point tp;
    while( !_stop ) {
      
      for (auto d=demux.begin(); d!=demux.end(); ++d) {
        tp = std::chrono::system_clock::now();
        while ( do_write && ((std::chrono::system_clock::now() - tp) < duration) ) {
          value = streamer[d->topic()].write(*d);
          if( value.ts() <= 0 )
            break;
        }
        
      }
    }
  }
  
  static std::chrono::milliseconds duration;

  BrightnESS::FileWriter::ProcessMessageResult value;
  std::map<std::string,Streamer> streamer;
  std::vector<Demux>& demux;  
  std::map< std::string,std::vector< std::pair<std::string,int64_t> > > timestamp_list;
  std::thread loop;
  std::atomic<ESSTimeStamp> stop_time;
  std::atomic<int> index;
  std::atomic<bool> do_write;
  std::atomic<bool> _stop;
  std::unique_ptr<FileWriterTask> file_writer_task_;
};


template<typename S,typename D>
std::chrono::milliseconds StreamMaster<S,D>::duration=10_ms;
