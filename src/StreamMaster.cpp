#include "StreamMaster.h"
#include "FileWriterTask.h"
#include "KafkaW/ConsumerFactory.h"
#include "Streamer.h"
#include "helper.h"
#include <condition_variable>

namespace FileWriter {

std::unique_ptr<StreamMaster>
StreamMaster::createStreamMaster(const std::string &Broker,
                                 std::unique_ptr<FileWriterTask> FileWriterTask,
                                 const MainOpt &Options) {
  std::map<std::string, Streamer> Streams;
  for (auto &Demux : FileWriterTask->demuxers()) {
    try {
      std::unique_ptr<KafkaW::ConsumerInterface> Consumer =
          KafkaW::createConsumer(Options.StreamerConfiguration.BrokerSettings,
                                 Broker);
      Streams.emplace(std::piecewise_construct,
                      std::forward_as_tuple(Demux.topic()),
                      std::forward_as_tuple(Broker, Demux.topic(),
                                            Options.StreamerConfiguration,
                                            std::move(Consumer)));
    } catch (std::exception &E) {
      getLogger()->critical("{}", E.what());
    }
  }

  return std::make_unique<StreamMaster>(std::move(FileWriterTask),
                                        Options.ServiceID, std::move(Streams));
}

StreamMaster::StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
                           std::string const &ServiceID,
                           std::map<std::string, Streamer> Streams)
    : NumStreamers(Streams.size()), Streamers(std::move(Streams)),
      WriterTask(std::move(FileWriterTask)), ServiceId(ServiceID) {}

StreamMaster::~StreamMaster() {
  Stop = true;
  if (WriteThread.joinable()) {
    WriteThread.join();
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

void StreamMaster::processStream(Streamer &Stream, DemuxTopic &Demux) {
  auto ProcessStartTime = std::chrono::system_clock::now();
  FileWriter::ProcessMessageResult ProcessResult;

  // Consume and process messages
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

void StreamMaster::run() {
  RunStatus.store(StreamMasterError::RUNNING);
  while (!Stop) {
    for (auto &Demux : WriterTask->demuxers()) {
      auto &s = Streamers[Demux.topic()];
      processStream(s, Demux);
    }
  }
  RunStatus.store(StreamMasterError::HAS_FINISHED);
  doStop();
}

void StreamMaster::closeStream(Streamer &Stream, const std::string &TopicName) {
  if (Stream.runStatus() != Status::StreamerStatus::HAS_FINISHED) {
    // Only decrement active streamer count if we haven't already marked it as
    // finished
    NumStreamers--;
    Logger->info(
        "Stopped streamer consuming from {}. {} streamers still running.",
        TopicName, NumStreamers);
  }
  Stream.close();

  if (NumStreamers == 0) {
    // No more streams open, so stop the StreamMaster
    Stop = true;
  }
}

void StreamMaster::doStop() {
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
