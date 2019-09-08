// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

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
  static std::unique_ptr<StreamMaster> createStreamMaster(
      const std::string &Broker, std::unique_ptr<FileWriterTask> FileWriterTask,
      const MainOpt &Options, std::shared_ptr<KafkaW::ProducerTopic> Producer) {

    std::map<std::string, Streamer> Streams;

    for (auto &Demux : FileWriterTask->demuxers()) {
      try {
        std::unique_ptr<KafkaW::ConsumerInterface> Consumer =
            KafkaW::createConsumer(Options.StreamerConfiguration.BrokerSettings,
                                   Broker);
        Streams.emplace(std::piecewise_construct,
                        std::forward_as_tuple(Demux.topic()),
                        std::forward_as_tuple(Broker, Demux.topic(),
                                              Options.StreamerConfiguration,
                                              std::move(Consumer)));
        Streams[Demux.topic()].setSources(Demux.sources());
      } catch (std::exception &E) {
        getLogger()->critical("{}", E.what());
        logEvent(Producer, StatusCode::Error, Options.ServiceID,
                 FileWriterTask->jobID(), E.what());
      }
    }

    return std::make_unique<StreamMaster>(
        std::move(FileWriterTask), Options.ServiceID, std::move(Producer),
        std::move(Streams));
  }

  StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
               std::string const &ServiceID,
               std::shared_ptr<KafkaW::ProducerTopic> Producer,
               std::map<std::string, Streamer> Streams)
      : NumStreamers(Streams.size()), Streamers(std::move(Streams)),
        WriterTask(std::move(FileWriterTask)), ServiceId(ServiceID),
        ProducerTopic(std::move(Producer)) {}

  StreamMaster(const StreamMaster &) = delete;
  StreamMaster(StreamMaster &&) = delete;
  StreamMaster &operator=(const StreamMaster &) = delete;
  StreamMaster &operator=(StreamMaster &&) = delete;

  ~StreamMaster() {
    Stop = true;
    if (WriteThread.joinable()) {
      WriteThread.join();
    }
    if (ReportThread.joinable()) {
      ReportThread.join();
    }
    Logger->info("Stopped StreamMaster for file with id : {}", getJobId());
  }

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
    return RunStatus.load() == Status::StreamMasterError::IS_REMOVABLE;
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

      // if the Streamer throws the stream is closed, but the file writing
      // continues
      try {
        ProcessResult = Stream.pollAndProcess(Demux);
      } catch (std::exception &E) {
        Logger->error("Stream closed due to stream error: {}", E.what());
        logEvent(ProducerTopic, StatusCode::Error, ServiceId,
                 WriterTask->jobID(), E.what());
        closeStream(Stream, Demux.topic());
        return;
      }

      // We've reached the stop offsets, we can close the stream
      if (ProcessResult == ProcessMessageResult::STOP) {
        closeStream(Stream, Demux.topic());
        return;
      } else if (ProcessResult == ProcessMessageResult::ERR) {
        // if there's any error in the messages log it
        Logger->info("Topic \"{}\" : {}", Demux.topic(),
                     Err2Str(Stream.runStatus()));
        return;
      }
    }
  }

  /// \brief Main loop that handles the writer process for each stream.
  void run() {
    RunStatus.store(StreamMasterError::RUNNING);
    while (!Stop) {
      for (auto &Demux : WriterTask->demuxers()) {
        auto &s = Streamers[Demux.topic()];
        processStreamResult(s, Demux);
      }
    }
    RunStatus.store(StreamMasterError::HAS_FINISHED);
    doStop();
  }

  /// \brief Close the Kafka connection in the specified stream.
  ///
  /// \param Stream The stream to close.
  void closeStream(Streamer &Stream, const std::string &TopicName) {
    if (Stream.runStatus() != Status::StreamerStatus::HAS_FINISHED) {
      // Only decrement active streamer count if we haven't already marked it as
      // finished
      NumStreamers--;
      Logger->info(
          "Stopped streamer consuming from {}. {} streamers still running.",
          TopicName, NumStreamers);
    }
    Stream.close();
  }

  /// \brief Stops the streamers and prepares for being removed.
  void doStop() {
    if (ReportThread.joinable()) {
      ReportThread.join();
    }
    for (auto &Stream : Streamers) {
      // Give the streams a chance to close, log if they fail
      Logger->info("Shutting down {}", Stream.first);
      Logger->info("Shut down {}", Stream.first);
      auto CloseResult = Stream.second.close();
      if (CloseResult != StreamerStatus::HAS_FINISHED) {
        Logger->info("Problem with stopping {} : {}", Stream.first,
                     Status::Err2Str(CloseResult));
      } else {
        Logger->info("Stopped {}", Stream.first);
      }
    }
    Streamers.clear();
    RunStatus.store(StreamMasterError::IS_REMOVABLE);
    Logger->debug("StreamMaster is removable");
  }

  size_t NumStreamers{0};
  std::map<std::string, Streamer> Streamers;
  std::thread WriteThread;
  std::thread ReportThread;
  std::atomic<StreamMasterError> RunStatus{StreamMasterError::OK};
  std::atomic<bool> Stop{false};
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  std::unique_ptr<Report> ReportPtr{nullptr};
  std::chrono::milliseconds TopicWriteDuration{1000};
  std::string ServiceId;
  std::shared_ptr<KafkaW::ProducerTopic> ProducerTopic;
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
