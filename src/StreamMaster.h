/// \file Header only implementation of the StreamMaster, that
/// coordinates the execution of the Streamers

#pragma once

#include "EventLogger.h"
#include "FileWriterTask.h"
#include "MainOpt.h"
#include "Report.h"

#include <atomic>
#include <condition_variable>

namespace FileWriter {

/// \brief The StreamMaster task is coordinate the different Streamers.
///
/// When constructed creates a unique Streamer per topic and waits for a
/// start command. When this command is issued the StreamMaster calls
/// the write command sequentially on each Streamer. When the stop
/// command is issued, or when the Steamer reaches a predetermined
/// point in time the Streamer is stopped and removed. The
/// StreamMaster can regularly send report on the status of the Streamers,
/// the amount of data written and other information as Kafka messages on
/// the ``status`` topic.
template <typename Streamer> class StreamMaster {
  using StreamerStatus = Status::StreamerStatus;
  using StreamMasterError = Status::StreamMasterError;
  friend class CommandHandler;

public:
  StreamMaster(const std::string &Broker,
               std::unique_ptr<FileWriterTask> FileWriterTask,
               const MainOpt &Options,
               std::shared_ptr<KafkaW::ProducerTopic> Producer)
      : Demuxers(FileWriterTask->demuxers()),
        WriterTask(std::move(FileWriterTask)), ServiceId{Options.ServiceID},
        ProducerTopic{Producer} {

    for (auto &Demux : Demuxers) {
      try {
        Streamers.emplace(std::piecewise_construct,
                          std::forward_as_tuple(Demux.topic()),
                          std::forward_as_tuple(Broker, Demux.topic(),
                                                Options.StreamerConfiguration));
        Streamers[Demux.topic()].setSources(Demux.sources());
      } catch (std::exception &E) {
        RunStatus = StreamMasterError::STREAMER_ERROR;
        Logger->critical("{}", E.what());
        logEvent(ProducerTopic, StatusCode::Error, ServiceId,
                 WriterTask->jobID(), E.what());
      }
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
    Logger->info("Stop StreamMaster for file with id : {}, ready to be removed",
                 getJobId());
  }

  StreamMaster &operator=(const StreamMaster &) = delete;

  /// \brief Set the point in time (in std::chrono::milliseconds) that triggers
  /// the termination of the run.
  ///
  /// When the timestamp of a Source in the
  /// Streamer reaches this time the source is removed. When all the
  /// Sources in a Streamer are removed the Streamer connection is
  /// closed and the Streamer marked as StreamerErrorCode::has_finished
  ///
  /// \param StopTime timestamp of the
  /// last message to be written in nanoseconds
  bool setStopTime(const std::chrono::milliseconds &StopTime) {
    for (auto &s : Streamers) {
      s.second.getOptions().StopTimestamp = StopTime;
    }
    return true;
  }

  /// Start writing the streams.
  ///
  /// \return True if successful, False otherwise.
  bool start() {
    if (NumStreamers == 0) {
      Stop = true;
      stopImplemented();
      return WriteThread.joinable();
    }
    Logger->info("StreamMaster: start");
    Stop = false;

    if (!WriteThread.joinable()) {
      WriteThread = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return WriteThread.joinable();
  }

  /// Stop writing the streams.
  ///
  /// \return True if successful, False otherwise.
  bool stop() {
    Logger->info("StreamMaster: stop");
    Stop = true;
    return !(WriteThread.joinable() || ReportThread.joinable());
  }

  void report(const std::chrono::milliseconds &ReportMs =
                  std::chrono::milliseconds{1000}) {
    if (NumStreamers != 0) {
      if (!ReportThread.joinable()) {
        ReportPtr.reset(
            new Report(ProducerTopic, WriterTask->jobID(), ReportMs));
        ReportThread =
            std::thread([&] { ReportPtr->report(Streamers, Stop, RunStatus); });
      } else {
        Logger->trace("Status report already started, nothing to do");
      }
    }
  }

  /// \brief Get FileWriterTask associated with the
  /// current file.
  ///
  /// \return Pointer to FileWriterTask.
  FileWriterTask const &getFileWriterTask() const { return *WriterTask; }

  /// \brief Get the current StreamMaster state, or
  /// StreamMasterError::streamer_error if any
  /// stream is in any error state.
  ///
  /// \return StreamMasterError status.
  const StreamMasterError status() {

    for (auto &s : Streamers) {
      if (s.second.runStatus() >= StreamerStatus::IS_CONNECTED) {
        return StreamMasterError::STREAMER_ERROR;
      }
    }
    return RunStatus.load();
  }

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  /// \return Job id as a string.
  std::string getJobId() const { return WriterTask->jobID(); }

private:
  /// \brief Process the messages in Stream for at most TopicWriteDuration
  /// std::chrono::milliseconds.
  ///
  /// \param Stream   A reference to the Streamer that will consume messages.
  /// \param Demux    The demux associated with the topic.
  ///
  /// \return The status of the consumption. If there are still working
  /// streams returns ``running``, if all the streams are terminated return
  /// ``has_finished``, if error was thrown returns ``streamer_error``.
  StreamMasterError processStreamResult(Streamer &Stream, DemuxTopic &Demux) {
    auto ProcessStartTime = std::chrono::system_clock::now();
    FileWriter::ProcessMessageResult ProcessResult;

    // process stream Stream fir at most TopicWriteDuration milliseconds
    while ((std::chrono::system_clock::now() - ProcessStartTime) <
           TopicWriteDuration) {
      if (Stop) {
        return StreamMasterError::HAS_FINISHED;
      }

      // if Streamer throws the stream is closed, but the writing continues
      try {
        ProcessResult = Stream.pollAndProcess(Demux);
      } catch (std::exception &E) {
        Logger->error("Stream closed due to stream error: {}", E.what());
        logEvent(ProducerTopic, StatusCode::Error, ServiceId,
                 WriterTask->jobID(), E.what());
        closeStream(Stream, Demux.topic());
        return StreamMasterError::STREAMER_ERROR;
      }
      // decreases the count of sources in the stream, eventually closes the
      // stream
      if (ProcessResult == ProcessMessageResult::STOP) {
        if (Stream.numSources() == 0) {
          return closeStream(Stream, Demux.topic());
        }
        return StreamMasterError::RUNNING;
      }
      // if there's any error in the messages logs it
      if (ProcessResult == ProcessMessageResult::ERR) {
        Logger->error("Error in topic \"{}\" : {}", Demux.topic(),
                      Err2Str(Stream.runStatus()));
        return StreamMasterError::STREAMER_ERROR;
      }
    }
    return StreamMasterError::RUNNING;
  }

  /// \brief Main loop that handles the writer process for each stream.
  ///
  /// The streams write as long as Stop is false and there are open streams. As
  /// the method starts the StreamMaster state is set to
  /// StreamMasterError::running. If a stream is in the SEC::writing state
  /// process the messages. If the state is SEC::has_finished or
  /// SEC::not_initialized skip the stream. A negative state represents an
  /// error, which is logged. When the method terminates (i.e. messages are not
  /// processed anymore) the StreamMaster state changes to
  /// StreamMasterError::has_finished.
  void run() {
    using namespace std::chrono;
    RunStatus = StreamMasterError::RUNNING;
    while (!Stop && NumStreamers > 0 && Demuxers.size() > 0) {

      for (auto &Demux : Demuxers) {
        auto &s = Streamers[Demux.topic()];

        // If the stream is active process the messages
        StreamMasterError ProcessResult = processStreamResult(s, Demux);
        if (ProcessResult == StreamMasterError::HAS_FINISHED) {
          continue;
        }
        if (ProcessResult == StreamMasterError::STREAMER_ERROR) {
          continue;
        }
      }
    }
    RunStatus = StreamMasterError::HAS_FINISHED;
    stopImplemented();
  }

  /// \brief Close the Kafka connection in the selected stream, set its value to
  /// SEC::has_finished and reduces the counter of the open streams.
  ///
  /// \return StreamMasterError::has_finished if other streams are open,
  /// otherwise set Stop TRUE and return StreamMasterError::has_finished.
  StreamMasterError closeStream(Streamer &Stream,
                                const std::string &TopicName) {
    Logger->trace("All sources in Stream have expired, close connection");
    Stream.runStatus() = Status::StreamerStatus::HAS_FINISHED;
    Stream.closeStream();
    NumStreamers--;
    if (NumStreamers != 0) {
      return StreamMasterError::RUNNING;
    }
    Stop = true;
    return StreamMasterError::HAS_FINISHED;
  }

  //   Implementation of the stop command. Make sure that the Streamers
  //   are not polled for messages, the status report is stopped and
  //   closes all the connections to the Kafka streams.
  void stopImplemented() {
    if (ReportThread.joinable()) {
      ReportThread.join();
    }
    for (auto &s : Streamers) {
      Logger->info("Shut down {}", s.first);
      auto v = s.second.closeStream();
      if (v == StreamerStatus::HAS_FINISHED) {
        Logger->info("Error while stopping {} : {}", s.first,
                     Status::Err2Str(v));
      } else {
        Logger->info("\t...done");
      }
    }
    Streamers.clear();
    RunStatus = StreamMasterError::IS_REMOVABLE;
    Logger->info("RunStatus:  {}", Err2Str(RunStatus));
  }

  std::map<std::string, Streamer> Streamers;
  std::vector<DemuxTopic> &Demuxers;
  std::thread WriteThread;
  std::thread ReportThread;
  std::atomic<StreamMasterError> RunStatus;
  std::atomic<bool> Stop{false};
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  std::unique_ptr<Report> ReportPtr{nullptr};
  std::chrono::milliseconds TopicWriteDuration{1000};
  size_t NumStreamers{0};
  std::string ServiceId;
  std::shared_ptr<KafkaW::ProducerTopic> ProducerTopic;
  std::shared_ptr<spdlog::logger> Logger = spdlog::get("filewriterlogger");
};

} // namespace FileWriter
