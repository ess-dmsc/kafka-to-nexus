#include "StreamController.h"
#include "FileWriterTask.h"
#include "Kafka/ConsumerFactory.h"
#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"
#include "Stream/Partition.h"
#include "helper.h"

namespace FileWriter {
StreamController::StreamController(
    std::unique_ptr<FileWriterTask> FileWriterTask,
    FileWriter::StreamerOptions const &Settings,
    Metrics::Registrar const &Registrar)

    : WriterTask(std::move(FileWriterTask)), StreamMetricRegistrar(Registrar),
      WriterThread([this]() { WriterTask->flushDataToFile(); },
                   Settings.DataFlushInterval,
                   Registrar.getNewRegistrar("stream")),
      KafkaSettings(Settings) {
  Executor.sendLowPriorityWork([=]() {
    CurrentMetadataTimeOut = Settings.BrokerSettings.MinMetadataTimeout;
    getTopicNames();
  });
}

StreamController::~StreamController() {
  // Hint streamers of exit
  LOG_INFO("Stopped StreamController for file with id : {}",
           StreamController::getJobId());
}

void StreamController::setStopTime(std::chrono::milliseconds const &StopTime) {
  KafkaSettings.StopTimestamp = time_point(StopTime);
  auto CStopTime = std::chrono::system_clock::time_point(StopTime);
  for (auto &s : Streamers) {
    s->setStopTime(CStopTime);
  }
}

void StreamController::stop() {
  for (auto &Stream : Streamers) {
    Stream->stop();
  }
  WriterThread.stop();
}

using duration = std::chrono::system_clock::duration;
bool StreamController::isDoneWriting() {
  return !StreamersRemaining.load() and
         KafkaSettings.StopTimestamp != time_point(duration(0)) and
         std::chrono::system_clock::now() > KafkaSettings.StopTimestamp;
}

std::string StreamController::getJobId() const { return WriterTask->jobID(); }

void StreamController::getTopicNames() {
  try {
    auto TopicNames = Kafka::getTopicList(KafkaSettings.BrokerSettings.Address,
                                          CurrentMetadataTimeOut);
    Executor.sendLowPriorityWork([=]() { initStreams(TopicNames); });
  } catch (MetadataException &E) {
    CurrentMetadataTimeOut *= 2;
    auto &Settings = KafkaSettings.BrokerSettings;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    LOG_WARN("Meta data call for retrieving topic names from the broker "
             "failed. Re-trying with a timeout of {} ms.",
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 CurrentMetadataTimeOut)
                 .count());
    Executor.sendLowPriorityWork([=]() { getTopicNames(); });
  }
}

void StreamController::initStreams(std::set<std::string> KnownTopicNames) {
  std::map<std::string, Stream::SrcToDst> TopicSrcMap;
  for (auto &Src : WriterTask->sources()) {
    if (KnownTopicNames.find(Src.topic()) != KnownTopicNames.end()) {
      TopicSrcMap[Src.topic()].push_back(
          {Src.getSrcHash(), Src.getModuleHash(), Src.getWriterPtr(),
           Src.sourcename(), Src.flatbufferID(), Src.writerModuleID(),
           Src.getWriterPtr()->acceptsRepeatedTimestamps()});
    } else {
      LOG_ERROR("Unable to set up consumer for source {} on topic {} as this "
                "topic does not exist.",
                Src.sourcename(), Src.topic());
    }
  }
  for (auto &CItem : TopicSrcMap) {
    auto CStartTime =
        std::chrono::system_clock::time_point(KafkaSettings.StartTimestamp);
    auto CStopTime =
        std::chrono::system_clock::time_point(KafkaSettings.StopTimestamp);
    auto CTopic = std::make_unique<Stream::Topic>(
        KafkaSettings.BrokerSettings, CItem.first, CItem.second, &WriterThread,
        StreamMetricRegistrar, CStartTime, KafkaSettings.BeforeStartTime,
        CStopTime, KafkaSettings.AfterStopTime);
    CTopic->start();
    Streamers.emplace_back(std::move(CTopic));
  }
  Executor.sendLowPriorityWork([=]() { checkIfStreamsAreDone(); });
}
using std::chrono_literals::operator""ms;
void StreamController::checkIfStreamsAreDone() {
  Streamers.erase(
      std::remove_if(Streamers.begin(), Streamers.end(),
                     [](auto const &Elem) { return Elem->isDone(); }),
      Streamers.end());

  if (Streamers.empty()) {
    StreamersRemaining.store(false);
  }
  std::this_thread::sleep_for(50ms);
  Executor.sendLowPriorityWork([=]() { checkIfStreamsAreDone(); });
}

} // namespace FileWriter
