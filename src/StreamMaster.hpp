#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <algorithm>
#include <atomic>
#include <thread>

#include "DemuxTopic.h"
#include "FileWriterTask.h"
#include "logger.h"
#include "utils.h"

struct Streamer;
struct FileWriterCommand;

namespace BrightnESS {
namespace FileWriter {

template <typename Streamer, typename Demux> struct StreamMaster {

  StreamMaster() : do_write(false), _stop(false){};

  StreamMaster(
      std::string &broker, std::vector<Demux> &_demux,
      std::vector<std::pair<std::string, std::string>> kafka_options = {},
      const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(_demux), do_write(false), _stop(false) {

    for (auto &d : demux) {
      streamer.emplace(d.topic(), Streamer{broker, d.topic(), kafka_options});
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  };

  StreamMaster(
      std::string &broker, std::unique_ptr<FileWriterTask> file_writer_task,
      std::vector<std::pair<std::string, std::string>> kafka_options = {},
      const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(file_writer_task->demuxers()), do_write(false), _stop(false),
        _file_writer_task(std::move(file_writer_task)) {

    for (auto &d : demux) {
      streamer.emplace(d.topic(), Streamer{broker, d.topic(), kafka_options});
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  };

  ~StreamMaster() {
    _stop = true;
    if (loop.joinable()) {
      loop.join();
    }
  }

  bool start_time(const ESSTimeStamp &start) {
    for (auto &d : demux) {
      auto result = streamer[d.topic()].set_start_time(d, start);
    }
    return false;
  }
  bool stop_time(const ESSTimeStamp &stop) {
    if (stop.count() < 0) {
      return false;
    }
    for (auto &d : demux) {
      d.stop_time() = stop.count();
    }
    return true;
  }

  bool start() {
    do_write = true;
    _stop = false;

    if (!loop.joinable()) {
      loop = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(milliseconds(100));
    }
    return loop.joinable();
  }

  bool stop() {
    do_write = false;
    _stop = true;
    if (loop.joinable()) {
      loop.join();
    }
    for (auto &d : demux) {
      this->stop_streamer(streamer[d.topic()]);
      streamer.erase(d.topic());
    }
    return !loop.joinable();
  }

  std::map<std::string, typename Streamer::status_type> &&status() {
    std::map<std::string, typename Streamer::status_type> stat;
    for (auto &s : streamer) {
      stat.emplace(s.first, s.second.status());
    }
    return std::move(stat);
  }

private:
  ErrorCode stop_streamer(const std::string &topic) {
    return streamer[topic].closeStream();
  }
  ErrorCode stop_streamer(Streamer &s) { return s.closeStream(); }

  void run() {
    using namespace std::chrono;
    system_clock::time_point tp, tp_global(system_clock::now());

    while (!_stop) {

      for (auto &d : demux) {
        auto &s = streamer[d.topic()];
        if (s.run_status() == StatusCode::RUNNING) {
          tp = system_clock::now();
          while (do_write && ((system_clock::now() - tp) < duration)) {
            auto _value = s.write(d);
            if (_value.is_STOP() &&
                (remove_source(d.topic()) != StatusCode::RUNNING)) {
              break;
            }
          }
        }
      }
      double total_size(0);
      for (auto &s : streamer) {
        total_size += s.second.status()["status.size"];
      }
      std::cout << "Written " << total_size * 1e-6 << "MB @ "
                << total_size * 1e3 / (system_clock::now() - tp_global).count()
                << "MB/s\n";
    }
  }

  ErrorCode remove_source(const std::string &topic) {
    auto &s(streamer[topic]);
    if (s.n_sources() > 1) {
      s.n_sources()--;
      return ErrorCode(StatusCode::RUNNING);
    } else {
      LOG(3, "All sources in {} have expired, remove streamer", topic);
      stop_streamer(s);
      streamer.erase(topic);
      return ErrorCode(StatusCode::STOPPED);
    }
  }

  std::map<std::string, Streamer> streamer;
  std::vector<Demux> &demux;
  std::thread loop;
  std::atomic<bool> do_write;
  std::atomic<bool> _stop;
  std::unique_ptr<FileWriterTask> _file_writer_task;

  milliseconds duration{1000};
};

} // namespace FileWriter
} // namespace BrightnESS
