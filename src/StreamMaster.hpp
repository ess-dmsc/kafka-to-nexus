#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <algorithm>
#include <atomic>
#include <exception>
#include <queue>
#include <thread>

#include "DemuxTopic.h"
#include "FileWriterTask.h"
//#include "KafkaW.h"
#include "Report.hpp"
#include "logger.h"

struct FileWriterCommand;
namespace KafkaW {
class ProducerTopic;
}

namespace FileWriter {

template <typename Streamer, typename Demux> class StreamMaster {
  using ErrorCode = Status::StreamMasterErrorCode;
  using Error = StreamMasterError;
  using Options = typename Streamer::Options;

public:
  StreamMaster() {}

  StreamMaster(const std::string &broker, std::vector<Demux> &_demux,
               const Options &kafka_options = {},
               const Options &filewriter_options = {})
      : demux(_demux) {

    for (auto &d : demux) {
      streamer.emplace(std::piecewise_construct,
                       std::forward_as_tuple(d.topic()),
                       std::forward_as_tuple(broker, d.topic(), kafka_options,
                                             filewriter_options));
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  }

  StreamMaster(const std::string &broker,
               std::unique_ptr<FileWriterTask> file_writer_task,
               const Options &kafka_options = {},
               const Options &filewriter_options = {})
      : demux(file_writer_task->demuxers()),
        _file_writer_task(std::move(file_writer_task)) {

    for (auto &d : demux) {
      streamer.emplace(std::piecewise_construct,
                       std::forward_as_tuple(d.topic()),
                       std::forward_as_tuple(broker, d.topic(), kafka_options,
                                             filewriter_options));
      streamer[d.topic()].n_sources() = d.sources().size();
    }
  }

  ~StreamMaster() {
    stop_ = true;
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
    stop_ = false;

    if (!loop.joinable()) {
      loop = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(milliseconds(100));
    }
    return loop.joinable();
  }

  int stop() {
    try {
      std::call_once(stop_once_guard,
                     &FileWriter::StreamMaster<Streamer, Demux>::stop_impl,
                     this);
    } catch (std::exception &e) {
      LOG(0, "Error while stopping: {}", e.what());
    }
    return !(loop.joinable() || report_thread_.joinable());
  }

  void report(std::shared_ptr<KafkaW::ProducerTopic> p,
              const int &delay = 1000) {
    if (delay < 0) {
      LOG(2, "Required negative delay in statistics collection: use default");
      return report(p);
    }
    if (!report_thread_.joinable()) {
      report_.reset(new Report(p, delay));
      report_thread_ =
	std::thread([&] { report_->report(streamer, stop_, runstatus); });
    } else {
      LOG(5, "Status report already started, nothing to do");
    }
  };

  FileWriterTask const &file_writer_task() const { return *_file_writer_task; }

  const Error status() {
    for (auto &s : streamer) {
      if (s.second.runstatus().value() < 0) {
        runstatus = ErrorCode::streamer_error;
      }
    }
    return Error{runstatus.load()};
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
    runstatus = ErrorCode::running;
    system_clock::time_point tp;

    while (!stop_) {
      for (auto &d : demux) {
        auto &s = streamer[d.topic()];
        if (s.runstatus().value() == Streamer::ErrorCode::writing) {
          tp = system_clock::now();
          while (do_write && ((system_clock::now() - tp) < duration)) {
            auto _value = s.write(d);
            if (_value.is_STOP() &&
                (remove_source(d.topic()) != Error{ErrorCode::running})) {
              break;
            }
          }
          continue;
        }
        if (s.runstatus().value() == Streamer::ErrorCode::not_initialized) {
          std::this_thread::sleep_for(duration);
          continue;
        }
        if (s.runstatus().value() < 0 &&
            s.runstatus().value() != Streamer::ErrorCode::not_initialized) {
          runstatus = ErrorCode::streamer_error;
          LOG(0, "Error in topic {} : {}", d.topic(), s.runstatus().value());
          remove_source(d.topic());
          continue;
        }
      }
    }
    runstatus = ErrorCode::has_finished;
  }

  Error remove_source(const std::string &topic) {
    auto &s(streamer[topic]);
    if (s.n_sources() > 1) {
      s.n_sources()--;
      return Error{ErrorCode::running};
    } else {
      LOG(3, "All sources in {} have expired, remove streamer", topic);
      stop_streamer(s);
      streamer.erase(topic);
      if (streamer.size() != 0) {
        return Error{ErrorCode::empty_streamer};
      } else {
        runstatus = ErrorCode::has_finished;
        stop_ = true;
        return Error{ErrorCode::has_finished};
      }
    }
  }

  void stop_impl() {
    LOG(7, "StreamMaster: stop");
    do_write = false;
    stop_ = true;
    if (loop.joinable()) {
      loop.join();
    }
    if (report_thread_.joinable()) {
      report_thread_.join();
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
  }

  std::map<std::string, Streamer> streamer;
  std::vector<Demux> &demux;
  std::thread loop;
  std::thread report_thread_;
  std::atomic<int> runstatus{ErrorCode::not_started};
  std::atomic<bool> do_write{false};
  std::atomic<bool> stop_{false};
  std::unique_ptr<FileWriterTask> _file_writer_task{nullptr};
  std::once_flag stop_once_guard;
  std::unique_ptr<Report> report_{nullptr};

  milliseconds duration{1000};
};

} // namespace FileWriter
