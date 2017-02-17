#pragma once

#include <iostream>
#include <string>
#include <functional>
#include <memory>
#include <vector>
#include <map>

#include <thread>
#include <atomic>
#include <algorithm>

#include "FileWriterTask.h"
#include "DemuxTopic.h"
#include "utils.h"

struct Streamer;
struct FileWriterCommand;


using namespace BrightnESS::FileWriter;

template<typename Streamer, typename Demux>
struct StreamMaster {
  static std::chrono::milliseconds delay_after_last_message;
  
  StreamMaster() : do_write(false), _stop(false) { };
  
  StreamMaster(std::string& broker, std::vector<Demux>& _demux) : demux(_demux), do_write(false), _stop(false) {
    for( auto& d: demux) {
      streamer[d.topic()] = Streamer(broker,d.topic());
    }
    loop = std::thread( [&] { this->run(); } );
  };

  StreamMaster(std::string& broker, std::unique_ptr<FileWriterTask> file_writer_task) :
    demux(file_writer_task->demuxers()), do_write(false), _stop(false), file_writer_task_(std::move(file_writer_task)) {
    for( auto& d: demux) {
      streamer[d.topic()] = Streamer(broker,d.topic());
    }
    loop = std::thread( [&] { this->run(); } );
  };

  ~StreamMaster() {
    _stop=true;
    if( loop.joinable() )
      loop.join();
  }

  
  bool start() {
    do_write = true;
    _stop = false;
    if( !loop.joinable() ) {
      loop = std::thread( [&] { this->run(); } );
      std::this_thread::sleep_for(100_ms);
    }
    return loop.joinable();
  }

  bool start(const ESSTimeStamp ts) {
    do_write = true;
    _stop = false;
    if( !loop.joinable() ) {
      loop = std::thread( [&] { this->run(); } );
      std::this_thread::sleep_for(100_ms);
    }
    return loop.joinable();
  }

  // Stops data writing. If a ESSTimeStamp threshold is given keeps writing
  // until the first packet with timestamp above threshold. Since then keep
  // receiving for delay_after_last_message milliseconds, then stops.
  std::vector< std::pair<std::string,int> > stop(const ESSTimeStamp ts=-2) {
    // If no message has been read so far, value.ts() will be == -1.
    // Therefore, use ts == -2 to indicate that we do *not* want to wait
    // for a certain time in the future.
    while( value.ts() < ts ) {
      std::this_thread::sleep_for(50_ms);
    }
    std::this_thread::sleep_for(delay_after_last_message);
    return stop_impl();
  }

  bool poll_n_messages(const int n) { return false; }
  
private:


  std::vector< std::pair<std::string,int> > stop_impl() {
    do_write = false;
    _stop = true;
    if( loop.joinable() ) loop.join();
    std::vector< std::pair<std::string,int> > stream_status;
    for (auto it=streamer.begin(); it!=streamer.end(); ++it) {
      stream_status.push_back(std::pair<std::string,int>(it->first,it->second.closeStream()));
    }
    return stream_status;
  }

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

  void find_initial_offset(ESSTimeStamp ts) {


  }



  
  static milliseconds duration;

  BrightnESS::FileWriter::ProcessMessageResult value;
  std::map<std::string,Streamer> streamer;
  std::vector<Demux>& demux;  
  std::map< std::string,std::vector< std::pair<std::string,ESSTimeStamp> > > timestamp_list;
  std::thread loop;
  std::atomic<ESSTimeStamp> stop_time;
  std::atomic<int> index;
  std::atomic<bool> do_write;
  std::atomic<bool> _stop;
  std::unique_ptr<FileWriterTask> file_writer_task_;
};


template<typename S,typename D>
milliseconds StreamMaster<S,D>::duration=10_ms;
template<typename S,typename D>
milliseconds StreamMaster<S,D>::delay_after_last_message=1000_ms;


// template<typename Streamer, typename Demux>
// class NaiveTimestampSearch {
// public:
//   typedef typename std::pair<std::string,ESSTimeStamp> value_t;
//   OffsetSearch(const ESSTimeStamp initial_time, Streamer& stream, Demux& demux) : _initial(initial_time),
//                                                                                   _stream(stream),
//                                                                                   _demux(demux) {

//     for(auto& _source : _stream.source() )
//       timestamp_list.push_back(0,_source.source());
//   }

//   ESSTimeStamp get() {
//     _stream.search_backward(_demux);
//     std::map m;
    
//   }
  
// private:
//   std::vector< value_t > timestamp_list;
//   ESSTimeStamp _initial;
//   Streamer& _stream;
//   Demux& _demux;
  
//   ProcessMessageResult_DT get_impl() {
//     return 
//   }

  
// };
