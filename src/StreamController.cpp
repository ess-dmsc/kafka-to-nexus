#include "StreamController.h"
#include "FileWriterTask.h"
#include "Kafka/ConsumerFactory.h"
#include "Streamer.h"
#include "helper.h"

namespace FileWriter {

std::unique_ptr<StreamController> StreamController::createStreamController(
    const std::string &Broker, std::unique_ptr<FileWriterTask> FileWriterTask,
    const MainOpt &Options) {
  std::map<std::string, Streamer> Streams;
  for (auto &TopicNameDemuxerPair : FileWriterTask->demuxers()) {
    try {
      std::unique_ptr<Kafka::ConsumerInterface> Consumer =
          Kafka::createConsumer(Options.StreamerConfiguration.BrokerSettings,
                                Broker);
      Streams.emplace(std::piecewise_construct,
                      std::forward_as_tuple(TopicNameDemuxerPair.first),
                      std::forward_as_tuple(Broker, TopicNameDemuxerPair.first,
                                            Options.StreamerConfiguration,
                                            std::move(Consumer),
                                            TopicNameDemuxerPair.second));
    } catch (std::exception &E) {
      getLogger()->critical("{}", E.what());
    }
  }

  return std::make_unique<StreamController>(
      std::move(FileWriterTask), Options.ServiceID, std::move(Streams));
}

StreamController::StreamController(
    std::unique_ptr<FileWriterTask> FileWriterTask,
    std::string const &ServiceID, std::map<std::string, Streamer> Streams)

    : Streamers(std::move(Streams)), WriterTask(std::move(FileWriterTask)),
      ServiceId(ServiceID) {}

StreamController::~StreamController() {
  Stop = true;
  if (WriteThread.joinable()) {
    WriteThread.join();
  }
  Logger->info("Stopped StreamController for file with id : {}", getJobId());
}

void StreamController::setStopTime(std::chrono::milliseconds const &StopTime) {
  for (auto &s : Streamers) {
    s.second.setStopTime(StopTime);
  }
}

void StreamController::setTopicWriteDuration(
    std::chrono::milliseconds Duration) {
  TopicWriteDuration = Duration;
}

void StreamController::start() {
  Logger->info("StreamController: start");
  Stop = false;

  if (!WriteThread.joinable()) {
    WriteThread = std::thread([&] { this->run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void StreamController::processStream(Streamer &Stream) {
  auto ProcessStartTime = std::chrono::system_clock::now();

  // Consume and process messages
  while ((std::chrono::system_clock::now() - ProcessStartTime) <
         TopicWriteDuration) {
    if (Stop) {
      return;
    }

    // if the Streamer throws then set the stream to finished, but the file
    // writing continues
    try {
      Stream.process();
    } catch (std::exception &E) {
      Logger->error("Stream closed due to stream error: {}", E.what());
      Stream.setFinished();
      return;
    }
  }
}

void StreamController::run() {

  while (!Stop && StreamersRemaining.load()) {
    bool StreamsStillWriting = false;

    for (auto &TopicStreamerPair : Streamers) {
      if (TopicStreamerPair.second.runStatus() !=
          StreamerStatus::HAS_FINISHED) {
        StreamsStillWriting = true;
        processStream(TopicStreamerPair.second);
      }
    }
    // Only update once we know the final answer to avoid race conditions
    StreamersRemaining.store(StreamsStillWriting);
  }

  doStop();
}

void StreamController::doStop() {
  for (auto &Stream : Streamers) {
    // Give the streams a chance to close, log if they fail
    Logger->info("Shutting down {}", Stream.first);
    Logger->info("Shut down {}", Stream.first);
    auto CloseResult = Stream.second.close();
    if (CloseResult != StreamerStatus::HAS_FINISHED) {
      Logger->info("Problem with stopping {} : {}", Stream.first,
                   StatusDescription(CloseResult));
    } else {
      Logger->info("Stopped {}", Stream.first);
    }
  }

  Streamers.clear();
}

bool StreamController::isDoneWriting() { return !StreamersRemaining.load(); }

std::string StreamController::getJobId() const { return WriterTask->jobID(); }

} // namespace FileWriter
