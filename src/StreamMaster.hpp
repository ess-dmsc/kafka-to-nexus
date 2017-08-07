#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <algorithm>
#include <atomic>
#include <queue>
#include <thread>

#include "DemuxTopic.h"
#include "FileWriterTask.h"
#include "KafkaW.h"
#include "Status.hpp"
#include "StatusWriter.hpp"
#include "logger.h"
#include "utils.h"

struct Streamer;
struct FileWriterCommand;

namespace FileWriter {

template <typename Streamer, typename Demux> class StreamMaster {
public:
  StreamMaster()
      : runstatus{Status::RunStatusError::not_started}, do_write{false},
        _stop{false} {};

  StreamMaster(
      std::string &broker, std::vector<Demux> &_demux,
      std::vector<std::pair<std::string, std::string>> kafka_options = {},
      const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(_demux), runstatus{Status::RunStatusError::not_started},
        do_write(false), _stop(false) {

    for (auto &d : demux) {
      streamer.emplace(d.topic(), Streamer{broker, d.topic(), kafka_options});
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  };

  StreamMaster(
      std::string &broker, std::unique_ptr<FileWriterTask> file_writer_task,
      std::vector<std::pair<std::string, std::string>> kafka_options = {},
      const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(file_writer_task->demuxers()),
        runstatus{Status::RunStatusError::not_started}, do_write(false),
        _stop(false), _file_writer_task(std::move(file_writer_task)) {

    for (auto &d : demux) {
      streamer.emplace(d.topic(), Streamer{broker, d.topic(), kafka_options});
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  };

  ~StreamMaster() {
    _stop = true;
    //    runstatus = Status::RunStatusError::has_finished;
    if (loop.joinable()) {
      loop.join();
    }
    if (fetch_statistics.joinable()) {
      fetch_statistics.join();
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

  void report(std::shared_ptr<KafkaW::ProducerTopic> p,
              const int &delay = 1000) {
    _report = p;
    if (!fetch_statistics.joinable()) {
      if (delay < 0) {
        LOG(2,
            "Required negative delay in statistics collection: nothing to do");
        return;
      }
      fetch_statistics = std::thread(
          std::bind(&StreamMaster<Streamer, Demux>::fetch_statistics_impl, this,
                    std::ref(_report), std::ref(delay)));
    }
    return;
  };

  FileWriterTask const &file_writer_task() const { return *_file_writer_task; }

private:
  ErrorCode stop_streamer(const std::string &topic) {
    return streamer[topic].closeStream();
  }
  ErrorCode stop_streamer(Streamer &s) { return s.closeStream(); }

  void run() {
    using namespace std::chrono;
    runstatus = Status::RunStatusError::writing;
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
    }
    runstatus = Status::RunStatusError::has_finished;
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
      if (streamer.size() == 0) {
        runstatus = Status::RunStatusError::has_finished;
      }
      return ErrorCode(StatusCode::STOPPED);
    }
  }

  void fetch_statistics_impl(std::shared_ptr<KafkaW::ProducerTopic> p,
                             const int &delay) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    while (!_stop) {
      Status::StreamMasterStatus status(runstatus.load());
      for (auto &s : streamer) {
        auto v = s.second.status();
        status.push(s.first, v.fetch_status(), v.fetch_statistics());
      }
      auto value = Status::pprint<Status::JSONStreamWriter>(status);
      std::cout << &value[0] << "\t" <<value.size() << std::endl;
      _report->produce(reinterpret_cast<unsigned char *>(&value[0]),
                       value.size());
      std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }
  }

  std::map<std::string, Streamer> streamer;
  std::vector<Demux> &demux;
  std::thread loop;
  std::thread fetch_statistics;
  std::atomic<int> runstatus;
  std::atomic<bool> do_write;
  std::atomic<bool> _stop;
  std::unique_ptr<FileWriterTask> _file_writer_task;
  std::shared_ptr<KafkaW::ProducerTopic> _report;

  milliseconds duration{1000};
}; // namespace FileWriter

} // namespace FileWriter
