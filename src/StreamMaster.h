// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file Header file for the StreamMaster, that
/// coordinates the execution of the Streamers

#pragma once

#include "Errors.h"
#include "EventLogger.h"
#include "MainOpt.h"
#include "Report.h"
#include <atomic>

namespace FileWriter {
class Streamer;
class FileWriterTask;

class IStreamMaster {
public:
  virtual ~IStreamMaster() = default;
  virtual std::string getJobId() const = 0;
  virtual void setStopTime(const std::chrono::milliseconds &StopTime) = 0;
  virtual nlohmann::json getStats() const = 0;
  virtual bool isDoneWriting() = 0;
};

/// \brief The StreamMaster's task is to coordinate the different Streamers.
class StreamMaster : public IStreamMaster {
  using StreamMasterError = Status::StreamMasterError;

public:
  /// \brief Builder method for StreamMaster.
  ///
  /// \param Broker The Kafka broker for the consumers.
  /// \param FileWriterTask The file-writer.
  /// \param Options The general application settings.
  /// \param Producer The Kafka producer used for reporting.
  /// \return
  static std::unique_ptr<StreamMaster> createStreamMaster(
      const std::string &Broker, std::unique_ptr<FileWriterTask> FileWriterTask,
      const MainOpt &Options, std::shared_ptr<KafkaW::ProducerTopic> Producer);

  StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
               std::string const &ServiceID,
               std::shared_ptr<KafkaW::ProducerTopic> Producer,
               std::map<std::string, Streamer> Streams);
  ~StreamMaster() override;
  StreamMaster(const StreamMaster &) = delete;
  StreamMaster(StreamMaster &&) = delete;
  StreamMaster &operator=(const StreamMaster &) = delete;
  StreamMaster &operator=(StreamMaster &&) = delete;

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
  void setStopTime(const std::chrono::milliseconds &StopTime) override;

  /// \brief Sets the write duration for the streams.
  ///
  /// \param Duration The duration to set.
  void setTopicWriteDuration(std::chrono::milliseconds Duration);

  /// Start writing the streams.
  void start();

  bool isDoneWriting() override;

  /// \brief Start the reporting thread.
  ///
  /// \param ReportMs How often to report.
  void report(const std::chrono::milliseconds &ReportMs =
                  std::chrono::milliseconds{1000});

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  ///
  /// \return The job id.
  std::string getJobId() const override { return WriterTask->jobID(); }

  nlohmann::json getStats() const override { return WriterTask->stats(); }

private:
  /// \brief Process the messages in the specified stream.
  ///
  /// \param Stream The stream that will consume messages.
  /// \param Demux The demux associated with the topic.
  void processStream(Streamer &Stream, DemuxTopic &Demux);

  /// \brief Main loop that handles the writer process for each stream.
  void run();

  /// \brief Stops the streamers and prepares for being removed.
  void doStop();

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
