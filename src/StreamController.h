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

#include "FileWriterTask.h"
#include "Kafka/MetaDataQuery.h"
#include "MainOpt.h"
#include "MetaData/HDF5DataWriter.h"
#include "MetaData/Tracker.h"
#include "Metrics/Registrar.h"
#include "Stream/Topic.h"
#include "ThreadedExecutor.h"
#include "TimeUtility.h"
#include "WriterModule/mdat/mdat_Writer.h"
#include <atomic>
#include <set>
#include <vector>

namespace FileWriter {

class IStreamController {
public:
  virtual ~IStreamController() = default;
  virtual std::string getJobId() const = 0;
  virtual void setStopTime(const time_point &StopTime) = 0;
  virtual bool isDoneWriting() = 0;
  virtual void pauseStreamers() = 0;
  virtual void resumeStreamers() = 0;
  virtual void start() = 0;
  virtual void stop() = 0;
  virtual bool hasErrorState() const = 0;
  virtual std::string errorMessage() = 0;
};

/// \brief The StreamController's task is to coordinate the different Streamers.
class StreamController : public IStreamController {
public:
  StreamController(
      std::unique_ptr<FileWriterTask> FileWriterTask,
      std::unique_ptr<WriterModule::mdat::mdat_Writer> mdatWriter,
      FileWriter::StreamerOptions const &Settings,
      Metrics::IRegistrar *Registrar, MetaData::TrackerPtr Tracker,
      std::shared_ptr<Kafka::MetadataEnquirer> metadata_enquirer,
      std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory);
  ~StreamController() override;
  StreamController(const StreamController &) = delete;
  StreamController(StreamController &&) = delete;
  StreamController &operator=(const StreamController &) = delete;
  StreamController &operator=(StreamController &&) = delete;

  /// \brief Start the streamers and, hence, the filewriting.
  ///
  /// MUST BE CALLED AFTER CONSTRUCTION!
  void start() override;

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
  void setStopTime(const time_point &StopTime) override;

  /// \brief Pause consumers.
  ///
  /// Pauses consumer polling to throttle the ingestion of data.
  void pauseStreamers() override;

  /// \brief Resume consumers.
  ///
  /// Resumes consumers if they were paused.
  void resumeStreamers() override;

  /// \brief Stop the streams as soon as possible.
  ///
  /// This call is not blocking but will trigger open streams to stop as soon as
  /// possible.
  void stop() override final;

  /// \brief Returns true if all topics are done AND current system time
  /// is greater than stop time.
  ///
  /// Will trigger a re-calculation of the approximate file size of the
  /// file-writing job is not done. But only every x number of seconds as
  /// hardcoded in this header file.
  ///
  /// \note If stop time has not been set, it will be treated as the maximum
  /// possible time.
  bool isDoneWriting() override;

  bool hasErrorState() const override;

  std::string errorMessage() override;

  /// \brief Get the unique job id associated with the streamer (and hence
  /// with the NeXus file).
  ///
  /// \return The job id.
  std::string getJobId() const override;

private:
  bool StopNow{false};
  void getTopicNames();
  void initStreams(std::set<std::string> known_topic_names);
  void performPeriodicChecks();
  void checkIfStreamsAreDone();
  void throttleIfWriteQueueIsFull();

  std::chrono::system_clock::duration CurrentMetadataTimeOut{};
  std::atomic<bool> StreamersRemaining{true};
  std::atomic<bool> StreamersPaused{false};
  std::atomic<bool> HasError{false};
  std::mutex ErrorMsgMutex;
  std::string ErrorMessage;
  duration const PeriodicChecksInterval{50ms};
  duration const FileSizeCalcInterval{5s};
  time_point LastFileSizeCalcTime{system_clock::now() - FileSizeCalcInterval};

  /// \brief Hysteresis factor to start refilling the write queue after a pause.
  ///
  /// Consumers are stopped when the write queue is larger than
  /// StreamerOptions.MaxQueuedWrites. This variable defines the ratio below
  /// which the consumers will be resumed.
  float const QueuedWritesResumeThreshold{0.8F};

  /// \brief The file-writing task object
  /// \note Must be located before the streamers and the writer thread to
  /// guarantee that its destructor is not called before the writer modules have
  /// been de-allocated.
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  std::unique_ptr<WriterModule::mdat::mdat_Writer> MdatWriter{nullptr};
  std::vector<std::unique_ptr<Stream::Topic>> Streamers;
  std::unique_ptr<Metrics::IRegistrar> StreamMetricRegistrar;
  Stream::MessageWriter WriterThread;
  FileWriter::StreamerOptions StreamerOptions;
  MetaData::TrackerPtr MetaDataTracker;
  std::shared_ptr<Kafka::MetadataEnquirer> _metadata_enquirer;
  std::shared_ptr<Kafka::ConsumerFactoryInterface> _consumer_factory;
  ThreadedExecutor Executor{false, "stream_controller"}; // Must be last
};

} // namespace FileWriter
