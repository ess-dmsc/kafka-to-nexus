//===-- src/StreamMaster.h - Streamers manager class definition -------*- C++
//-*-===//
//
//
//===----------------------------------------------------------------------===//
///
/// \file Header only implementation of the StreamMaster, that
/// coordinates the execution of the Streamers
///
//===----------------------------------------------------------------------===//

#pragma once

#include "FileWriterTask.h"
#include "Report.hpp"

#include <atomic>
#include <condition_variable>

namespace FileWriter {
class StreamerOptions;
}

namespace FileWriter {

/// The StreamMaster task is coordinate the different Streamers. When
/// constructed creates a unique Streamer per topic and waits for a
/// start command. When this command is issued the StreamMaster calls
/// the write command sequentially on each Streamer. When the stop
/// command is issued, or when the Steamer reaches a predetermined
/// point in time the Streamer is stopped and removed.  The
/// StreamMaster can regularly send report on the status of the Streamers,
/// the amount of data written and other information as Kafka messages on
/// the ``status`` topic.
template <typename Streamer> class StreamMaster {
  using SEC = Status::StreamerErrorCode;
  using SMEC = Status::StreamMasterErrorCode;
  friend class CommandHandler;

public:
  StreamMaster(const std::string &broker,
               std::unique_ptr<FileWriterTask> file_writer_task,
               const StreamerOptions &options)
      : Demuxers(file_writer_task->demuxers()),
        WriterTask(std::move(file_writer_task)) {

    for (auto &d : Demuxers) {
      Streamers.emplace(std::piecewise_construct,
                        std::forward_as_tuple(d.topic()),
                        std::forward_as_tuple(broker, d.topic(), options));
      Streamers[d.topic()].setSources(d.sources());
    }
    NumStreamers = Streamers.size();
  }

  StreamMaster(const StreamMaster &) = delete;
  StreamMaster(StreamMaster &&) = default;

  ~StreamMaster() {
    Stop = true;
    if (WriteThread.joinable()) {
      WriteThread.join();
    }
    if (ReportThread.joinable()) {
      ReportThread.join();
    }
  }

  StreamMaster &operator=(const StreamMaster &) = delete;

  /// Set the timepoint (in milliseconds) that triggers the
  /// termination of the run. When the timestamp of a Source in the
  /// Streamer reaches this time the source is removed. When all the
  /// Sources in a Streamer are removed the Streamer connection is
  /// closed and the Streamer marked as
  /// StreamerErrorCode::has_finished
  /// \param StopTime timestamp of the
  /// last message to be written in nanoseconds
  bool setStopTime(const milliseconds &StopTime) {
    for (auto &s : Streamers) {
      s.second.getOptions().StopTimestamp = StopTime;
    }
    return true;
  }

  /// Start the streams writing. Return true if successful, false
  /// in case of failure
  bool start() {
    LOG(Sev::Info, "StreamMaster: start");
    Stop = false;

    if (!WriteThread.joinable()) {
      WriteThread = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(milliseconds(100));
    }
    return WriteThread.joinable();
  }

  /// Stop the streams writing. Return true if successful, false in case
  /// of failure.
  bool stop() {
    LOG(Sev::Info, "StreamMaster: stop");
    Stop = true;
    return !(WriteThread.joinable() || ReportThread.joinable());
  }

  void report(std::shared_ptr<KafkaW::ProducerTopic> p,
              const milliseconds &report_ms = milliseconds{1000}) {
    if (!ReportThread.joinable()) {
      ReportPtr.reset(new Report(p, report_ms));
      ReportThread =
          std::thread([&] { ReportPtr->report(Streamers, Stop, RunStatus); });
    } else {
      LOG(Sev::Debug, "Status report already started, nothing to do");
    }
  }

  /// Return a reference to the FileWriterTask associated with the
  /// current file
  FileWriterTask const &getFileWriterTask() const { return *WriterTask; }

  /// Returns the current StreamMaster state, or SMEC::streamer_error if any
  /// stream is in any error state
  const SMEC status() {
    for (auto &s : Streamers) {
      if (int(s.second.runStatus()) < 0) {
        return SMEC::streamer_error;
      }
    }
    return RunStatus.load();
  }

  /// Return the unique job id associated with the streamer (and hence
  /// with the NeXus file)
  std::string getJobId() const { return WriterTask->job_id(); }

private:
  /// Process the messages in Stream for at most TopicWriteDuration
  /// milliseconds. If the TopicWriteDuration time is elapsed or the stream is
  /// stopped and other stream are active the method returns SMEC::running. If
  /// all the streams have finished returns SMEC::has_finished.
  SMEC processStreamResult(Streamer &Stream, DemuxTopic &Demux) {
    auto ProcessStartTime = std::chrono::system_clock::now();
    while ((std::chrono::system_clock::now() - ProcessStartTime) <
           TopicWriteDuration) {
      if (Stop) {
        return SMEC::has_finished;
      }
      FileWriter::ProcessMessageResult ProcessResult = Stream.write(Demux);
      if (ProcessResult.is_STOP()) {
        if (Stream.numSources() == 0) {
          return closeStream(Stream, Demux.topic());
        }
        return SMEC::running;
      }
    }
    return SMEC::running;
  }

  /// Main loop that handles the writer process for each stream. The streams
  /// write as long as Stop is false and there are open streams. As the method
  /// starts the StreamMaster state is set to SMEC::running. If a
  /// stream is in the SEC::writing state process the messages. If the state is
  /// SEC::has_finished or SEC::not_initialized skip the stream. A negative
  /// state represents an error, which is logged. When the method terminates
  /// (i.e. messages are not processed anymore) the StreamMaster state changes
  /// to SMEC::has_finished.
  void run() {
    using namespace std::chrono;
    RunStatus = SMEC::running;
    while (!Stop && NumStreamers > 0 && Demuxers.size() > 0) {

      for (auto &Demux : Demuxers) {
        auto &s = Streamers[Demux.topic()];
        // If the stream is active process the messages
        if (s.runStatus() == SEC::writing) {
          SMEC ProcessResult = processStreamResult(s, Demux);
          if (ProcessResult == SMEC::has_finished) {
            break;
          }
          continue;
        }
        // If the stream has finished skip to the next stream. Breaking the
        // allow the re-evaluation of NumStreamers
        if (s.runStatus() == SEC::has_finished) {
          continue;
        }
        // If the Kafka connection is not ready skip to the next stream.
        // Nevertheless if there's only one stream wait some time in order
        // to prevent spinning
        if (s.runStatus() == SEC::not_initialized) {
          if (Streamers.size() == 1) {
            std::this_thread::sleep_for(milliseconds(500));
          }
          continue;
        }
        if (int(s.runStatus()) < 0) {
          LOG(Sev::Error, "Error in topic {} : {}", Demux.topic(),
              int(s.runStatus()));
          continue;
        }
      }
    }
    RunStatus = SMEC::has_finished;
    stopImplemented();
  }

  /// Close the Kafka connection in the selected stream, set its value to
  /// SEC::has_finished and reduces the counter of the open streams. If there
  /// are other open streams return SMEC::has_finished, else Stop becomes true
  /// and return SMEC::has_finished
  SMEC closeStream(Streamer &Stream, const std::string &TopicName) {
    LOG(Sev::Debug, "All sources in Stream have expired, close connection");
    Stream.runStatus() = SEC::has_finished;
    Stream.closeStream();
    NumStreamers--;
    if (NumStreamers != 0) {
      return SMEC::running;
    }
    Stop = true;
    return SMEC::has_finished;
  }

  /// Implementation of the stop command. Make sure that the Streamers
  /// are not polled for messages, the status report is stopped and
  /// closes all the connections to the Kafka streams.
  void stopImplemented() {
    if (ReportThread.joinable()) {
      ReportThread.join();
    }
    for (auto &s : Streamers) {
      LOG(Sev::Info, "Shut down {}", s.first);
      auto v = s.second.closeStream();
      if (v != SEC::has_finished) {
        LOG(Sev::Warning, "Error while stopping {} : {}", s.first,
            Status::Err2Str(v));
      } else {
        LOG(Sev::Info, "\t...done");
      }
    }
    Streamers.clear();
    RunStatus = SMEC::is_removable;
    LOG(Sev::Info, "RunStatus:  {}", Err2Str(RunStatus));
  }

  std::map<std::string, Streamer> Streamers;
  std::vector<DemuxTopic> &Demuxers;
  std::thread WriteThread;
  std::thread ReportThread;
  std::atomic<SMEC> RunStatus{SMEC::not_started};
  std::atomic<bool> Stop{false};
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  std::unique_ptr<Report> ReportPtr{nullptr};
  milliseconds TopicWriteDuration{1000};
  size_t NumStreamers{0};
};

} // namespace FileWriter
