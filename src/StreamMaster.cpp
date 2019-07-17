#include "StreamMaster.h"
#include "FileWriterTask.h"
#include "Streamer.h"
#include "helper.h"

#include <KafkaW/ConsumerFactory.h>
#include <condition_variable>

namespace FileWriter {

StreamMaster::StreamMaster(const std::string &Broker,
                           std::unique_ptr<FileWriterTask> FileWriterTask,
                           const MainOpt &Options,
                           std::shared_ptr<KafkaW::ProducerTopic> Producer)
    : Demuxers(FileWriterTask->demuxers()),
      RunStatus(Status::StreamMasterError::NOT_STARTED),
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
      logEvent(ProducerTopic, StatusCode::Error, ServiceId, WriterTask->jobID(),
               E.what());
    }
  }
  NumStreamers = Streamers.size();
}

StreamMaster::~StreamMaster() {
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

/// Set the point in time that triggers
/// the termination of the run.
///
/// When the timestamp of a Source in the
/// Streamer reaches this time the source is removed. When all the
/// Sources in a Streamer are removed the Streamer connection is
/// closed and the Streamer marked as finished.
///
/// \param StopTime Timestamp of the
/// last message to be written in nanoseconds.
void StreamMaster::setStopTime(const std::chrono::milliseconds &StopTime) {
  for (auto &s : Streamers) {
    s.second.getOptions().StopTimestamp = StopTime;
  }
}

/// Start writing the streams.
void StreamMaster::start() {
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
void StreamMaster::requestStop() {
  Logger->info("StreamMaster: stop requested");
  Stop = true;
}

void StreamMaster::report(const std::chrono::milliseconds &ReportMs) {
  if (NumStreamers != 0) {
    if (!ReportThread.joinable()) {
      ReportPtr = std::make_unique<Report>(ProducerTopic, WriterTask->jobID(),
                                           ReportMs);
      ReportThread =
          std::thread([&] { ReportPtr->report(Streamers, Stop, RunStatus); });
    } else {
      Logger->trace("Status report already started, nothing to do");
    }
  }
}

/// \brief Get whether this stream master can be removed.
///
/// \return True, if can be removed.
bool StreamMaster::isRemovable() const {

  bool StreamStillConnected =
      std::any_of(Streamers.begin(), Streamers.end(), [](auto &Item) {
        return Item.second.runStatus() >= StreamerStatus::IS_CONNECTED;
      });

  return !StreamStillConnected &&
         RunStatus.load() == Status::StreamMasterError::IS_REMOVABLE;
}

/// \brief Process the messages in Stream for at most TopicWriteDuration
/// std::chrono::milliseconds.
///
/// \param Stream The Streamer that will consume messages.
/// \param Demux The demux associated with the topic.
void StreamMaster::processStreamResult(Streamer &Stream, DemuxTopic &Demux) {
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
      logEvent(ProducerTopic, StatusCode::Error, ServiceId, WriterTask->jobID(),
               E.what());
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
///
/// The streams write as long as Stop is false and there are open streams.
void StreamMaster::run() {
  RunStatus = StreamMasterError::RUNNING;
  while (!Stop && NumStreamers > 0 && !Demuxers.empty()) {
    for (auto &Demux : Demuxers) {
      auto &Stream = Streamers[Demux.topic()];
      if (Stream.runStatus() != StreamerStatus::HAS_FINISHED) {
        processStreamResult(Stream, Demux);
      }
    }
  }
  RunStatus = StreamMasterError::HAS_FINISHED;
  doStop();
}

/// \brief Close the Kafka connection in the specified stream.
///
/// \param Stream The stream to close.
void StreamMaster::closeStream(Streamer &Stream, const std::string &TopicName) {
  if (Stream.runStatus() != Status::StreamerStatus::HAS_FINISHED) {
    // Only decrement active streamer count if we haven't already marked it as
    // finished
    NumStreamers--;
    Logger->info(
        "Stopped streamer consuming from {}. {} streamers still running.",
        TopicName, NumStreamers);
  }
  Stream.closeStream();

  if (NumStreamers == 0) {
    // No more streams open, so stop
    Stop = true;
  }
}

/// \brief Stops the streamers and prepares for being removed.
void StreamMaster::doStop() {
  Logger->info("In doStop");
  if (ReportThread.joinable()) {
    ReportThread.join();
  }
  Logger->info("Joined ReportThread");
  for (auto &Stream : Streamers) {
    Logger->info("Shut down {}", Stream.first);
    auto CloseResult = Stream.second.closeStream();
    if (CloseResult != StreamerStatus::HAS_FINISHED) {
      Logger->info("While stopping {} : {}", Stream.first,
                   Status::Err2Str(CloseResult));
    } else {
      Logger->info("\t...done");
    }
  }
  Logger->info("Calling Streamers.clear() in doStop()");

  for (auto it = Streamers.cbegin(); it != Streamers.cend();
       /* no increment */) {
    Logger->info("Erasing Streamer {}", it->first);
    Streamers.erase(it++);
  }
  Logger->info("Erased all Streamers");

  Streamers.clear();
  RunStatus = StreamMasterError::IS_REMOVABLE;
  Logger->info("RunStatus:  {}", Err2Str(RunStatus));
}
} // namespace FileWriter
