/// \file Header only implementation of the StreamMaster, that
/// coordinates the execution of the Streamers

#pragma once

#include "EventLogger.h"
#include "FileWriterTask.h"
#include "MainOpt.h"
#include "Report.h"
#include "StreamerFactory.h"
#include "StreamerI.h"

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
class StreamMaster {
  using StreamerStatus = Status::StreamerStatus;
  using StreamMasterError = Status::StreamMasterError;

public:
  StreamMaster(const std::string &Broker,
               std::unique_ptr<FileWriterTask> FileWriterTask,
               const MainOpt &Options,
               std::unique_ptr<IStreamerFactory> StreamerFactory);
  StreamMaster(const StreamMaster &) = delete;
  StreamMaster(StreamMaster &&) = delete;

  ~StreamMaster();

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
  bool setStopTime(const std::chrono::milliseconds &StopTime);

  /// Start the streams writing. Return true if successful, false
  /// in case of failure
  bool start();

  /// Stop the streams writing. Return true if successful, false in case
  /// of failure.
  bool stop();

  /// \brief Get FileWriterTask associated with the
  /// current file.
  ///
  /// \return Pointer to FileWriterTask.
  FileWriterTask const &getFileWriterTask() const { return *WriterTask; }

  /// \brief Get the current StreamMaster state, or
  /// StreamMasterError::streamer_error if any
  /// stream is in any error state
  const StreamMasterError status();

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
  /// ``has_finished``, if some error occur..
  StreamMasterError processStreamResult(DemuxTopic &Demux);

  /// Main loop that handles the writer process for each stream. The streams
  /// write as long as Stop is false and there are open streams. As the method
  /// starts the StreamMaster state is set to StreamMasterError::running. If a
  /// stream is in the SEC::writing state process the messages. If the state is
  /// SEC::has_finished or SEC::not_initialized skip the stream. A negative
  /// state represents an error, which is logged. When the method terminates
  /// (i.e. messages are not processed anymore) the StreamMaster state changes
  /// to StreamMasterError::has_finished.
  void run();

  /// Close the Kafka connection in the selected stream, set its value to
  /// SEC::has_finished and reduces the counter of the open streams. If there
  /// are other open streams return StreamMasterError::has_finished, else Stop
  /// becomes true
  /// and return StreamMasterError::has_finished
  StreamMasterError closeStream(const std::string &TopicName);

  /// Implementation of the stop command. Make sure that the Streamers
  /// are not polled for messages, the status report is stopped and
  /// closes all the connections to the Kafka streams.
  void stopImplemented();

  void runReport(const std::chrono::milliseconds &ReportMs =
                     std::chrono::milliseconds{1000});

  std::map<std::string, std::unique_ptr<IStreamer>> Streamers;
  std::vector<DemuxTopic> &Demuxers;
  std::shared_ptr<KafkaW::ProducerTopic> ProducerTopic;
  std::unique_ptr<FileWriterTask> WriterTask{nullptr};
  std::thread WriteThread;
  std::thread ReportThread;
  std::atomic<StreamMasterError> RunStatus;
  std::atomic<bool> Stop{false};
  std::unique_ptr<Report> ReportPtr{nullptr};
  std::chrono::milliseconds ReportMessageDelay{1000};
  std::chrono::milliseconds TopicWriteDuration{1000};
  size_t NumStreamers{0};
  std::string ServiceId;
};

} // namespace FileWriter
