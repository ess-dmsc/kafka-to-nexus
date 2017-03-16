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
#include "logger.h"

struct Streamer;
struct FileWriterCommand;

namespace BrightnESS {
namespace FileWriter {

template<typename Streamer, typename Demux>
struct StreamMaster {
  static std::chrono::milliseconds delay_after_last_message;

  StreamMaster() : do_write(false), _stop(false) {};

  StreamMaster(std::string &broker, std::vector<Demux> &_demux,
               const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(_demux), do_write(false), _stop(false) {
    for (auto &d : demux) {
      streamer[d.topic()] = Streamer(broker, d.topic(), offset);
      _n_sources[d.topic()] = d.sources().size();
      _status[d.topic()] = ErrorCode(StatusCode::STOPPED);
    }
  };

  StreamMaster(std::string &broker,
               std::unique_ptr<FileWriterTask> file_writer_task,
               const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(file_writer_task->demuxers()), do_write(false), _stop(false),
        _file_writer_task(std::move(file_writer_task)) {
    for (auto &d : demux) {
      streamer[d.topic()] = Streamer(broker, d.topic(), offset);
      _n_sources[d.topic()] = d.sources().size();
      _status[d.topic()] = ErrorCode(StatusCode::STOPPED);
    }
  };

  ~StreamMaster() {
    _stop = true;
    if (loop.joinable())
      loop.join();
  }

  bool start_time(const ESSTimeStamp start) {
    for (auto &d : demux) {
      auto result = streamer[d.topic()].set_start_time(d, start);
      for (auto r : result) {
        std::cout << r.first << "\t:\t" << r.second << std::endl;
      }
    }
    exit(0);
    LOG(3, "StreamMaster > start_time :\t{}", start.count());
    return false;
  }
  bool stop_time(const ESSTimeStamp stop) {
    if (stop.count() < 0) {
      return false;
    }
    for (auto &d : demux) {
      d.stop_time() = stop.count();
    }
    LOG(3, "StreamMaster - stop_time :\t{}", stop.count());
    return true;
  }

  bool start() {
    do_write = true;
    _stop = false;
    for (auto item = _status.begin(); item != _status.end(); ++item) {
      item->second = ErrorCode(StatusCode::RUNNING);
    }
    if (!loop.joinable()) {
      loop = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(milliseconds(100));
    }
    return loop.joinable();
  }

  std::map<std::string, ErrorCode> &stop() {
    do_write = false;
    _stop = true;
    if (loop.joinable()) {
      loop.join();
    }
    for (auto &d : demux) {
      _status[d.topic()] = streamer[d.topic()].closeStream();
    }
    return _status;
  }

  std::map<std::string, ErrorCode> &status() { return _status; }

private:
  ErrorCode stop_streamer(const std::string &topic) {
    return streamer[topic].closeStream();
  }
  ErrorCode stop_streamer(Streamer &s) { return s.closeStream(); }

  void run() {
    std::chrono::system_clock::time_point tp;
    while (!_stop) {

      for (auto &d : demux) {
        // switch (_status[d.topic()].value()) {
        // case StatusCode::RUNNING:
        //   std::cout << d.topic() << " is running\n";
        //   break;
        // case StatusCode::STOPPED:
        //   std::cout << d.topic() << " is stopped\n";
        //   break;
        // case StatusCode::ERROR:
        //   std::cout << "Error in " << d.topic() << "\n";
        //   break;
        // default:
        //   break;
        // }
        if (_status[d.topic()].value()) {

          tp = std::chrono::system_clock::now();
          while (do_write &&
                 ((std::chrono::system_clock::now() - tp) < duration)) {
            auto _value = streamer[d.topic()].write(d);
            if (_value.is_STOP()) {

              if (remove_source(d.topic()) != ErrorCode(StatusCode::RUNNING)) {
                _status[d.topic()] = ErrorCode(StatusCode::STOPPED);
                break;
              }
            }
          }
          if (_status[d.topic()] == ErrorCode(StatusCode::STOPPED)) {
            _status[d.topic()] = streamer[d.topic()].closeStream();
          }
        }
        
      }
    }
  }
  
  ErrorCode remove_source(const std::string &s) {
    if (_n_sources[s] > 1) {
      _n_sources[s]--;
      return std::move(ErrorCode(StatusCode::RUNNING));
    }
    _n_sources[s] = 0;
    LOG(3, "All sources in {} have expired", s);
    return std::move(ErrorCode(StatusCode::STOPPED));
  }

  std::map<std::string, Streamer> streamer;
  std::map<std::string, int> _n_sources;
  std::map<std::string, ErrorCode> _status;

  std::vector<Demux> &demux;
  std::thread loop;
  std::atomic<bool> do_write;
  std::atomic<bool> _stop;
  std::unique_ptr<FileWriterTask> _file_writer_task;

  static milliseconds duration;
};

template <typename S, typename D>
milliseconds StreamMaster<S, D>::duration = milliseconds(10);
template <typename S, typename D>
milliseconds StreamMaster<S, D>::delay_after_last_message = milliseconds(1000);
}
}
