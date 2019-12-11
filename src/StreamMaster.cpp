#include "StreamMaster.h"
#include "FileWriterTask.h"
#include "KafkaW/ConsumerFactory.h"
#include "Streamer.h"
#include "helper.h"
#include <condition_variable>

namespace FileWriter {

std::unique_ptr<StreamMaster> StreamMaster::createStreamMaster(
    const std::string &Broker, std::unique_ptr<FileWriterTask> FileWriterTask,
    const MainOpt &Options, std::shared_ptr<KafkaW::ProducerTopic> Producer) {
  std::map<std::string, Streamer> Streams;
  for (auto &TopicNameDemuxerPair : FileWriterTask->demuxers()) {
    try {
      std::unique_ptr<KafkaW::ConsumerInterface> Consumer =
          KafkaW::createConsumer(Options.StreamerConfiguration.BrokerSettings,
                                 Broker);
      Streams.emplace(std::piecewise_construct,
                      std::forward_as_tuple(TopicNameDemuxerPair.first),
                      std::forward_as_tuple(Broker, TopicNameDemuxerPair.first,
                                            Options.StreamerConfiguration,
                                            std::move(Consumer)));
    } catch (std::exception &E) {
      getLogger()->critical("{}", E.what());
      logEvent(Producer, StatusCode::Error, Options.ServiceID,
               FileWriterTask->jobID(), E.what());
    }
  }

  return std::make_unique<StreamMaster>(std::move(FileWriterTask),
                                        Options.ServiceID, std::move(Producer),
                                        std::move(Streams));
}

StreamMaster::StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
                           std::string const &ServiceID,
                           std::shared_ptr<KafkaW::ProducerTopic> Producer,
                           std::map<std::string, Streamer> Streams)
    : NumStreamers(Streams.size()), Streamers(std::move(Streams)),
      WriterTask(std::move(FileWriterTask)), ServiceId(ServiceID),
      ProducerTopic(std::move(Producer)) {}

StreamMaster::~StreamMaster() {
  Stop = true;
  if (WriteThread.joinable()) {
    WriteThread.join();
  }
  if (ReportThread.joinable()) {
    ReportThread.join();
  }
  Logger->info("Stopped StreamMaster for file with id : {}", getJobId());
}

void StreamMaster::setStopTime(const std::chrono::milliseconds &StopTime) {
  for (auto &s : Streamers) {
    s.second.getOptions().StopTimestamp = StopTime;
  }
}

void StreamMaster::setTopicWriteDuration(std::chrono::milliseconds Duration) {
  TopicWriteDuration = Duration;
}

void StreamMaster::start() {
  Logger->info("StreamMaster: start");
  Stop = false;

  if (!WriteThread.joinable()) {
    WriteThread = std::thread([&] { this->run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
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

void StreamMaster::processStream(Streamer &Stream, DemuxTopic &Demux) {
  auto ProcessStartTime = std::chrono::system_clock::now();

  // Consume and process messages
  while ((std::chrono::system_clock::now() - ProcessStartTime) <
         TopicWriteDuration) {
    if (Stop) {
      return;
    }

    // if the Streamer throws then set the stream to finished, but the file
    // writing
    // continues
    try {
      Stream.pollAndProcess(Demux);
    } catch (std::exception &E) {
      Logger->error("Stream closed due to stream error: {}", E.what());
      logEvent(ProducerTopic, StatusCode::Error, ServiceId, WriterTask->jobID(),
               E.what());
      Stream.setFinished();
      return;
    }
  }
}

void StreamMaster::run() {
  RunStatus.store(StreamMasterError::RUNNING);
  bool StreamersLeft = true;
  while (!Stop && StreamersLeft) {
    StreamersLeft = false;
    for (auto &TopicStreamerPair : Streamers) {
      if (TopicStreamerPair.second.runStatus() !=
          Status::StreamerStatus::HAS_FINISHED) {
        processStream(TopicStreamerPair.second,
                      WriterTask->demuxers()[TopicStreamerPair.first]);
        StreamersLeft = true;
      }
    }
  }
  RunStatus.store(StreamMasterError::HAS_FINISHED);
  doStop();
}

void StreamMaster::doStop() {
  if (ReportThread.joinable()) {
    ReportThread.join();
  }
  for (auto &Stream : Streamers) {
    // Give the streams a chance to close, log if they fail
    Logger->info("Shutting down {}", Stream.first);
    Logger->info("Shut down {}", Stream.first);
    auto CloseResult = Stream.second.close();
    if (CloseResult != Status::StreamerStatus::HAS_FINISHED) {
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

bool StreamMaster::isDoneWriting() { return NumStreamers == 0; }
} // namespace FileWriter
