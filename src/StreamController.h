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
#include "Metrics/Registrar.h"
#include "Stream/Topic.h"
#include "ThreadedExecutor.h"
#include <atomic>
#include <set>
#include <vector>

namespace FileWriter {
class FileWriterTask;

class IStreamController {
public:
  virtual ~IStreamController() = default;
  virtual std::string getJobId() const = 0;
  virtual void setStopTime(const std::chrono::milliseconds &StopTime) = 0;
  virtual bool isDoneWriting() = 0;
  virtual void stop() = 0;
};

/// \brief The StreamController's task is to coordinate the different Streamers.
class StreamController : public IStreamController {
public:
  StreamController(std::unique_ptr<FileWriterTask> FileWriterTask,
                   FileWriter::StreamerOptions const &Settings,
                   Metrics::Registrar const &Registrar);
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

  /// \brief Stop the streams as soon as possible.
  ///
  /// This call is not blocking but will trigger open streams to stop as soon as
  /// possible.
  void stop() override;

  /// \brief Returns true if all topics are done AND current system time
  /// is greater than stop time.
  ///
  /// \note If stop time has not been set, it will be treated as the maximum
  /// possible time.
  bool isDoneWriting() override;

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  ///
  /// \return The job id.
  std::string getJobId() const override;

private:
  void getTopicNames();
  void initStreams(std::set<std::string> KnownTopicNames);
  void checkIfStreamsAreDone();
  std::chrono::system_clock::duration CurrentMetadataTimeOut;
  std::atomic<bool> StreamersRemaining{true};
  std::vector<std::unique_ptr<Stream::Topic>> Streamers;
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  Metrics::Registrar StreamMetricRegistrar;
  Stream::MessageWriter WriterThread;
  FileWriter::StreamerOptions KafkaSettings;
  ThreadedExecutor Executor; // Must be last
};

} // namespace FileWriter
