/// \file Header-only implementation of the StreamMaster, that
/// coordinates the execution of the Streamers

#pragma once

#include "EventLogger.h"
#include "FileWriterTask.h"
#include "MainOpt.h"
#include "Report.h"
#include "helper.h"

#include <KafkaW/ConsumerFactory.h>
#include <atomic>
#include <condition_variable>

namespace FileWriter {

/// \brief The StreamMaster's task is to coordinate the different Streamers.
template <typename Streamer> class StreamMaster {
  using StreamerStatus = Status::StreamerStatus;
  using StreamMasterError = Status::StreamMasterError;

public:
  StreamMaster(const std::string &Broker,
               std::unique_ptr<FileWriterTask> FileWriterTask,
               const MainOpt &Options,
               std::shared_ptr<KafkaW::ProducerTopic> Producer)
      : Demuxers(FileWriterTask->demuxers()),
        WriterTask(std::move(FileWriterTask)), ServiceId{Options.ServiceID},
        ProducerTopic{std::move(Producer)} {
    for (auto &Demux : Demuxers) {
      try {
        std::unique_ptr<KafkaW::ConsumerInterface> Consumer =
            KafkaW::createConsumer(Options.StreamerConfiguration.BrokerSettings,
                                   Broker);
        Streamers.emplace(std::piecewise_construct,
                          std::forward_as_tuple(Demux.topic()),
                          std::forward_as_tuple(Broker, Demux.topic(),
                                                Options.StreamerConfiguration,
                                                std::move(Consumer)));
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

  /// \brief Set the point in time that triggers
  /// the termination of the run.
  ///
  /// When the timestamp of a Source in the
  /// Streamer reaches this time the source is removed. When all the
  /// Sources in a Streamer are removed the Streamer connection is
  /// closed and the Streamer marked as finished.
  ///
  /// \param StopTime Timestamp of the
  /// last message to be written in nanoseconds.
  void setStopTime(const std::chrono::milliseconds &StopTime) {
    for (auto &s : Streamers) {
      s.second.getOptions().StopTimestamp = StopTime;
    }
  }

  void setTopicWriteDuration(std::chrono::milliseconds NewTopicWriteDuration) {
    TopicWriteDuration = NewTopicWriteDuration;
  }

  /// Start writing the streams.
  void start() {
    if (NumStreamers == 0) {
      Stop = true;
      doStop();
      return;
    }

    Logger->info("StreamMaster: start");
    Stop = false;

    if (!WriteThread.joinable()) {
      WriteThread = std::thread([&] { this->run(); });
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  /// Request to stop writing the streams.
  void requestStop() {
    Logger->info("StreamMaster: stop requested");
    Stop = true;
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

  /// \brief Get whether this stream master can be removed.
  ///
  /// \return True, if can be removed.
  bool isRemovable() const {

    bool StreamStillConnected = std::any_of(Streamers.begin(), Streamers.end(), [](auto & Item)
    {
      return Item.second.runStatus() >= StreamerStatus::IS_CONNECTED;
    });

    return !StreamStillConnected && RunStatus.load() == Status::StreamMasterError::IS_REMOVABLE;
  }

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  ///
  /// \return The job id.
  std::string getJobId() const { return WriterTask->jobID(); }

private:
  /// \brief Process the messages in Stream for at most TopicWriteDuration
  /// std::chrono::milliseconds.
  ///
  /// \param Stream The Streamer that will consume messages.
  /// \param Demux The demux associated with the topic.
  void processStreamResult(Streamer &Stream, DemuxTopic &Demux) {
    auto ProcessStartTime = std::chrono::system_clock::now();
    FileWriter::ProcessMessageResult ProcessResult;

    // process stream Stream for at most TopicWriteDuration milliseconds
    while ((std::chrono::system_clock::now() - ProcessStartTime) <
           TopicWriteDuration) {
      if (Stop) {
        return;
      }

      // if the Streamer throws the stream is closed, but the file writing continues
      try {
        ProcessResult = Stream.pollAndProcess(Demux);
      } catch (std::exception &E) {
        Logger->error("Stream closed due to stream error: {}", E.what());
        logEvent(ProducerTopic, StatusCode::Error, ServiceId,
                 WriterTask->jobID(), E.what());
        closeStream(Stream);
        return;
      }

      if (ProcessResult == ProcessMessageResult::STOP) {
        if (Stream.numSources() == 0) {
          closeStream(Stream);

        }
        return;
      }
      else if (ProcessResult == ProcessMessageResult::ERR) {
        // if there's any error in the messages log it
        Logger->error("Error in topic \"{}\" : {}", Demux.topic(),
                      Err2Str(Stream.runStatus()));
        return;
      }
    }
  }

  /// \brief Main loop that handles the writer process for each stream.
  ///
  /// The streams write as long as Stop is false and there are open streams.
  void run() {
    RunStatus = StreamMasterError::RUNNING;
    while (!Stop && NumStreamers > 0 && Demuxers.size() > 0) {
      for (auto &Demux : Demuxers) {
        auto &s = Streamers[Demux.topic()];
        processStreamResult(s, Demux);
      }
    }
    RunStatus = StreamMasterError::HAS_FINISHED;
    doStop();
  }

  /// \brief Close the Kafka connection in the specified stream.
  ///
  /// \param Stream The stream to close.
  void closeStream(Streamer &Stream) {
    Logger->trace("All sources in Stream have expired, close connection");
    Stream.closeStream();
    NumStreamers--;
    if (NumStreamers == 0) {
      // No more streams open, so stop
      Stop = true;
    }
  }

  /// \brief Stops the streamers and prepares for being removed.
  void doStop() {
    if (ReportThread.joinable()) {
      ReportThread.join();
    }
    for (auto &s : Streamers) {
      Logger->info("Shut down {}", s.first);
      auto v = s.second.closeStream();
      if (v != StreamerStatus::HAS_FINISHED) {
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
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
