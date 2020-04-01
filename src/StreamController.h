// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file Header file for the StreamController, that
/// coordinates the execution of the Streamers

#pragma once

#include "MainOpt.h"
#include <atomic>
#include <thread>

namespace FileWriter {
class Streamer;
class FileWriterTask;

class IStreamController {
public:
  virtual ~IStreamController() = default;
  virtual std::string getJobId() const = 0;
  virtual void setStopTime(const std::chrono::milliseconds &StopTime) = 0;
  virtual bool isDoneWriting() = 0;
};

/// \brief The StreamController's task is to coordinate the different Streamers.
class StreamController : public IStreamController {
public:
  /// \brief Builder method for StreamController.
  ///
  /// \param Broker The Kafka broker for the consumers.
  /// \param FileWriterTask The file-writer.
  /// \param Options The general application settings.
  /// \param Producer The Kafka producer used for reporting.
  /// \return
  static std::unique_ptr<StreamController>
  createStreamController(const std::string &Broker,
                         std::unique_ptr<FileWriterTask> FileWriterTask,
                         const MainOpt &Options);

  StreamController(std::unique_ptr<FileWriterTask> FileWriterTask,
                   std::string const &ServiceID,
                   std::map<std::string, Streamer> Streams);
  ~StreamController() override;
  StreamController(const StreamController &) = delete;
  StreamController(StreamController &&) = delete;
  StreamController &operator=(const StreamController &) = delete;
  StreamController &operator=(StreamController &&) = delete;

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

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  ///
  /// \return The job id.
  std::string getJobId() const override;

private:
  /// \brief Process the messages in the specified stream.
  ///
  /// \param Stream The stream that will consume messages.
  void processStream(Streamer &Stream);

  /// \brief Main loop that handles the writer process for each stream.
  void run();

  /// \brief Stops the streamers and prepares for being removed.
  void doStop();

  std::atomic<bool> StreamersRemaining{true};
  std::map<std::string, Streamer> Streamers;
  std::thread WriteThread;
  std::atomic<bool> Stop{false};
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  std::chrono::milliseconds TopicWriteDuration{1000};
  std::string ServiceId;
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
