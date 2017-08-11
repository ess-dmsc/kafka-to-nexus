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
  using Error = Status::StreamMasterErrorCode;

public:
  StreamMaster()
      : runstatus{Error::not_started}, do_write{false}, _stop{false} {};

  StreamMaster(
      std::string &broker, std::vector<Demux> &_demux,
      std::vector<std::pair<std::string, std::string>> kafka_options = {},
      const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(_demux), runstatus{Error::not_started}, do_write(false),
        _stop(false) {

    for (auto &d : demux) {
      // streamer.emplace(d.topic(), broker, d.topic(), kafka_options);
      streamer.emplace(std::piecewise_construct,
                       std::forward_as_tuple(d.topic()),
                       std::forward_as_tuple(broker, d.topic(), kafka_options));
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  };

  StreamMaster(
      std::string &broker, std::unique_ptr<FileWriterTask> file_writer_task,
      std::vector<std::pair<std::string, std::string>> kafka_options = {},
      const RdKafkaOffset &offset = RdKafkaOffsetEnd)
      : demux(file_writer_task->demuxers()), runstatus{Error::not_started},
        do_write(false), _stop(false),
        _file_writer_task(std::move(file_writer_task)) {

    for (auto &d : demux) {
      streamer.emplace(std::piecewise_construct,
                       std::forward_as_tuple(d.topic()),
                       std::forward_as_tuple(broker, d.topic(), kafka_options));
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  };

  ~StreamMaster() {
    _stop = true;
    //    runstatus = Status::RunStatusError::has_finished;
    if (loop.joinable()) {
      loop.join();
    }
    if (report_thread_.joinable()) {
      report_thread_.join();
    }
  }

  bool start_time(const ESSTimeStamp &start) {
    for (auto &s : streamer) {
      auto result = s.second.set_start_time(start);
      if (result.value() != Streamer::ErrorCode::no_error) {
        return false;
      }
    }
    return true;
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
    LOG(7, "StreamMaster: start");
    do_write = true;
    _stop = false;

    if (!loop.joinable()) {
      loop = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(milliseconds(100));
    }
    return loop.joinable();
  }

  bool stop() {
    std::lock_guard<std::mutex> lock(stop_guard_);
    LOG(7, "StreamMaster: stop");
    do_write = false;
    _stop = true;
    if (report_thread_.joinable()) {
      report_thread_.join();
    }
    if (loop.joinable()) {
      loop.join();
    }
    for (auto &s : streamer) {
      LOG(7, "Shut down {} : {}", s.first);
      auto v = stop_streamer(s.second);
      if (v != typename Streamer::Error(Streamer::ErrorCode::stopped)) {
        LOG(1, "Error while stopping {} : {}", s.first, Status::Err2Str(v));
      } else {
        LOG(7, "\t...done");
      }
    }
    streamer.clear();
    return !loop.joinable();
  }

  void report(std::shared_ptr<KafkaW::ProducerTopic> p,
              const int &delay = 1000) {
    report_producer_ = p;
    if (!report_thread_.joinable()) {
      if (delay < 0) {
        LOG(2, "Required negative delay in statistics collection: use default");
        return report(p);
      }
      report_thread_ = std::thread(
          std::bind(&StreamMaster<Streamer, Demux>::report_impl, this,
                    std::ref(report_producer_), std::ref(delay)));
    }
    return;
  };

  FileWriterTask const &file_writer_task() const { return *_file_writer_task; }

  const StreamMasterError &status() {
    for (auto &s : streamer) {
      if (s.second.runstatus().value() < 0) {
        runstatus = Error::streamer_error;
      }
    }
    return StreamMasterError{runstatus.load()};
  }

private:
  typename Streamer::Error stop_streamer(const std::string &topic) {
    return streamer[topic].closeStream();
  }
  typename Streamer::Error stop_streamer(Streamer &s) {
    return s.closeStream();
  }

  void run() {
    using namespace std::chrono;
    runstatus = Error::running;
    system_clock::time_point tp, tp_global(system_clock::now());

    while (!_stop) {

      for (auto &d : demux) {
        auto &s = streamer[d.topic()];
        if (s.runstatus().value() == Streamer::ErrorCode::writing) {
          tp = system_clock::now();
          while (do_write && ((system_clock::now() - tp) < duration)) {
            auto _value = s.write(d);
            //       runstatus.store(int(s.status()));
            if (_value.is_STOP() && (remove_source(d.topic()).value() !=
                                     Streamer::ErrorCode::stopped)) {
              break;
            }
          }
        }
        if (s.runstatus().value() == Streamer::ErrorCode::not_initialized) {
          std::this_thread::sleep_for(duration);
          continue;
        }
        if (s.runstatus().value() < 0 &&
            s.runstatus().value() != Streamer::ErrorCode::not_initialized) {
          runstatus = Error::streamer_error;
          LOG(0, "Error in topic {} : {}", d.topic(), s.runstatus().value());
          remove_source(d.topic());
          continue;
        }
      }
    }
    runstatus = Error::has_finished;
  }

  // ErrorCode remove_source(std::map<std::string, Streamer>::iterator &iter) {
  //   iter.second->remove_source();
  // }

  ErrorCode remove_source(const std::string &topic) {
    auto &s(streamer[topic]);
    if (s.n_sources() > 1) {
      s.n_sources()--;
      return Streamer::ErrorCode::writing;
    } else {
      LOG(3, "All sources in {} have expired, remove streamer", topic);
      stop_streamer(s);
      streamer.erase(topic);
      if (streamer.size() == 0) {
        runstatus = Error::has_finished;
      }
      return Streamer::ErrorCode::stopped;
    }
  }

  void report_impl(std::shared_ptr<KafkaW::ProducerTopic> p,
                   const int delay = 1000) {
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    while (!_stop) {
      Status::StreamMasterStatus status(runstatus.load());
      for (auto &s : streamer) {
        auto v = s.second.status();
        status.push(s.first, v.fetch_status(), v.fetch_statistics());
      }
      auto value = Status::pprint<Status::JSONStreamWriter>(status);
      if (!report_producer_) {
        LOG(1, "ProucerTopic error: can't produce StreamMaster status report")
        runstatus = Error::statistics_failure;
        return;
      }
      report_producer_->produce(reinterpret_cast<unsigned char *>(&value[0]),
                                value.size());
      std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }
    return;
  }

  std::map<std::string, Streamer> streamer;
  std::vector<Demux> &demux;
  std::thread loop;
  std::thread report_thread_;
  std::atomic<int> runstatus;
  std::atomic<bool> do_write;
  std::atomic<bool> _stop;
  std::unique_ptr<FileWriterTask> _file_writer_task;
  std::shared_ptr<KafkaW::ProducerTopic> report_producer_;
  std::mutex stop_guard_;

  milliseconds duration{1000};
}; // namespace FileWriter

} // namespace FileWriter
