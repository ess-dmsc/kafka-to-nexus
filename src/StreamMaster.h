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
#include "MainOpt.h"
#include "Report.h"

#include <atomic>

namespace FileWriter {
class Streamer;
class FileWriterTask;

/// The StreamMaster's task is to coordinate the different Streamers.
class StreamMaster {
  using StreamerStatus = Status::StreamerStatus;
  using StreamMasterError = Status::StreamMasterError;

public:
  StreamMaster(const std::string &Broker,
               std::unique_ptr<FileWriterTask> FileWriterTask,
               const MainOpt &Options,
               std::shared_ptr<KafkaW::ProducerTopic> Producer);

  ~StreamMaster();
  StreamMaster(const StreamMaster &) = delete;
  StreamMaster(StreamMaster &&) = default;
  StreamMaster &operator=(const StreamMaster &) = delete;

  /// Set the point in time that triggers
  /// the termination of the run.
  void setStopTime(const std::chrono::milliseconds &StopTime);

  void setTopicWriteDuration(std::chrono::milliseconds NewTopicWriteDuration) {
    TopicWriteDuration = NewTopicWriteDuration;
  }

  /// Start writing the streams.
  void start();

  /// Request to stop writing the streams.
  void requestStop();

  void report(const std::chrono::milliseconds &ReportMs =
                  std::chrono::milliseconds{1000});

  /// \brief Get FileWriterTask associated with the
  /// current file.
  ///
  /// \return Pointer to FileWriterTask.
  FileWriterTask const &getFileWriterTask() const { return *WriterTask; }

  /// Get whether this stream master can be removed.
  bool isRemovable() const;

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
  void processStreamResult(Streamer &Stream, DemuxTopic &Demux);

  /// \brief Main loop that handles the writer process for each stream.
  ///
  /// The streams write as long as Stop is false and there are open streams.
  void run();

  /// \brief Close the Kafka connection in the specified stream.
  ///
  /// \param Stream The stream to close.
  void closeStream(Streamer &Stream, const std::string &TopicName);

  /// \brief Stops the streamers and prepares for being removed.
  void doStop();

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
