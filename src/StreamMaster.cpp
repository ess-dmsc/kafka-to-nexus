#include "StreamMaster.h"
#include "FileWriterTask.h"
#include "KafkaW/ConsumerFactory.h"
#include "KafkaW/MetaDataQuery.h"
#include "KafkaW/MetadataException.h"
#include "Stream/Partition.h"
#include "helper.h"

namespace FileWriter {
StreamMaster::StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
                           std::string const &ServiceID,
                           FileWriter::StreamerOptions Settings,
                           Metrics::Registrar Registrar)

    : WriterTask(std::move(FileWriterTask)), StreamMetricRegistrar(Registrar),
      WriterThread(Registrar.getNewRegistrar("stream")), ServiceId(ServiceID),
      KafkaSettings(Settings) {
  Executor.SendWork([=]() {
    CurrentMetadataTimeOut = Settings.BrokerSettings.MinMetadataTimeout;
    getTopicNames();
  });
}

StreamMaster::~StreamMaster() {
  // Hint streamers of exit
  LOG_INFO("Stopped StreamMaster for file with id : {}", getJobId());
}

void StreamMaster::setStopTime(std::chrono::milliseconds const &StopTime) {
  auto CStopTime = std::chrono::system_clock::time_point(StopTime);
  for (auto &s : Streamers) {
    s->setStopTime(CStopTime);
  }
}

bool StreamMaster::isDoneWriting() { return !StreamersRemaining.load(); }

std::string StreamMaster::getJobId() const { return WriterTask->jobID(); }

void StreamMaster::getTopicNames() {
  try {
    auto TopicNames = KafkaW::getTopicList(KafkaSettings.BrokerSettings.Address,
                                           CurrentMetadataTimeOut);
    Executor.SendWork([=]() { initStreams(TopicNames); });
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
    Executor.SendLowPrioWork([=]() { getTopicNames(); });
  }
}

void StreamMaster::initStreams(std::set<std::string> KnownTopicNames) {
  std::map<std::string, Stream::SrcToDst> TopicSrcMap;
  for (auto &Src : WriterTask->sources()) {
    if (KnownTopicNames.find(Src.topic()) != KnownTopicNames.end()) {
      TopicSrcMap[Src.topic()].push_back(
          {Src.getHash(), reinterpret_cast<intptr_t>(Src.getWriterPtr()), Src.sourcename(), Src.flatbufferID()});
    } else {
      LOG_ERROR("Unable to set up consumer for source {} on topic {} as this "
                "topic is does not exist.",
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
        StreamMetricRegistrar, CStartTime, KafkaSettings.BeforeStartTime, CStopTime, KafkaSettings.AfterStopTime);
    Streamers.push_back(std::move(CTopic));
  }
}

} // namespace FileWriter
