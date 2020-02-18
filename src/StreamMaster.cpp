#include "StreamMaster.h"
#include "FileWriterTask.h"
#include "KafkaW/ConsumerFactory.h"
#include "KafkaW/MetaDataQuery.h"
#include "Streamer.h"
#include "helper.h"
#include "Stream/Partition.h"

namespace FileWriter {
StreamMaster::StreamMaster(std::unique_ptr<FileWriterTask> FileWriterTask,
                           std::string const &ServiceID,
                           FileWriter::StreamerOptions Settings, Metrics::Registrar Registrar)

    : WriterTask(std::move(FileWriterTask)), StreamMasterRegistrar(Registrar), WriterThread(Registrar.getNewRegistrar("stream")),
      ServiceId(ServiceID), KafkaSettings(Settings) {
  Executor.SendWork([=](){
    CurrentMetadataTimeOut = Settings.BrokerSettings.MinMetadataTimeout;
    getTopicNames();
  });
}

StreamMaster::~StreamMaster() {
  // Hint streamers of exit
  Logger->info("Stopped StreamMaster for file with id : {}", getJobId());
}

void StreamMaster::setStopTime(std::chrono::milliseconds const &StopTime) {
  for (auto &s : Streamers) {
    s.second.setStopTime(StopTime);
  }
}

bool StreamMaster::isDoneWriting() { return !StreamersRemaining.load(); }

std::string StreamMaster::getJobId() const { return WriterTask->jobID(); }

void StreamMaster::getTopicNames() {
  try {
    auto TopicNames = KafkaW::getTopicList(KafkaSettings.BrokerSettings.Address, CurrentMetadataTimeOut);
    Executor.SendWork([=](){
      initStreams(TopicNames);
    });
  } catch (MetadataException &E) {
    CurrentMetadataTimeOut *= 2;
    auto &Settings = KafkaSettings.BrokerSettings;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Logger->warn("Meta data call for retrieving topic names from the broker failed. Re-trying with a timeout of {} ms.", std::chrono::duration_cast<std::chrono::milliseconds>(CurrentMetadataTimeOut).count());
    Executor.SendWork([=]() {
      getTopicNames();
    });
  }
}

void StreamMaster::initStreams(std::set<std::string> KnownTopicNames) {
  std::map<std::string, Stream::SrcToDst> TopicSrcMap;
  for (auto &Src : WriterTask->sources()) {
    if (KnownTopicNames.find(Src.topic()) != KnownTopicNames.end()) {
      TopicSrcMap[Src.topic()].push_back({Src.getHash(), reinterpret_cast<intptr_t>(Src.getWriterPtr())});
    } else {
      Logger->error("Unable to set up consumer for source {} on topic {} as this topic is does not exist.", Src.sourcename(), Src.topic());
    }
  }
  for (auto &CItem : TopicSrcMap) {
    Streamers.emplace(KafkaSettings.BrokerSettings, CItem.first, CItem.second, &WriterThread, StreamMetricRegistrar, KafkaSettings.StartTime, KafkaSettings.StopTime);
  }
}

} // namespace FileWriter
