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

#include "EventLogger.h"
#include "MainOpt.h"
#include <atomic>
#include "ThreadedExecutor.h"
#include "Stream/Topic.h"
#include <set>
#include "Metrics/Registrar.h"

namespace FileWriter {
class Streamer;
class FileWriterTask;

class IStreamMaster {
public:
  virtual ~IStreamMaster() = default;
  virtual std::string getJobId() const = 0;
  virtual void setStopTime(const std::chrono::milliseconds &StopTime) = 0;
  virtual bool isDoneWriting() = 0;
};

/// \brief The StreamMaster's task is to coordinate the different Streamers.
class StreamMaster : public IStreamMaster {
public:
  StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
               std::string const &ServiceID, FileWriter::StreamerOptions Settings, Metrics::Registrar Registrar);
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

  bool isDoneWriting() override;

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  ///
  /// \return The job id.
  std::string getJobId() const override;

private:
  void getTopicNames();
  void initStreams(std::set<std::string> KnownTopicNames);
  std::chrono::system_clock::duration CurrentMetadataTimeOut;
  std::atomic<bool> StreamersRemaining{true};
  std::vector<std::unique_ptr<Stream::Topic>> Streamers;
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  Metrics::Registrar StreamMetricRegistrar;
  Stream::MessageWriter WriterThread;
  std::string ServiceId;
  FileWriter::StreamerOptions KafkaSettings;
  ThreadedExecutor Executor; // Must be last
};

} // namespace FileWriter
