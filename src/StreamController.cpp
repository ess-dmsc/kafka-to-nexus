#include "StreamController.h"
#include "FileWriterTask.h"
#include "HDF5/HDFOperations.h"
#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"
#include "Stream/Partition.h"
#include "TimeUtility.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {
StreamController::StreamController(
    std::unique_ptr<FileWriterTask> FileWriterTask,
    std::unique_ptr<WriterModule::mdat::mdat_Writer> mdatWriter,
    FileWriter::StreamerOptions const &Settings,
    Metrics::Registrar const &Registrar, MetaData::TrackerPtr const &Tracker)

    : WriterTask(std::move(FileWriterTask)), MdatWriter(std::move(mdatWriter)),
      StreamMetricRegistrar(Registrar),
      WriterThread([this]() { WriterTask->flushDataToFile(); },
                   Settings.DataFlushInterval,
                   Registrar.getNewRegistrar("stream")),
      StreamerOptions(Settings), MetaDataTracker(Tracker) {
  MdatWriter->setStartTime(Settings.StartTimestamp);
  MdatWriter->setStopTime(Settings.StopTimestamp);
  Executor.sendLowPriorityWork([=]() {
    CurrentMetadataTimeOut = Settings.BrokerSettings.MinMetadataTimeout;
    getTopicNames();
  });
}

StreamController::~StreamController() {
  stop();
  MdatWriter->writeMetadata(WriterTask.get());
  LOG_INFO("Stopped StreamController for file with id : {}",
           StreamController::getJobId());
}

void StreamController::setStopTime(time_point const &StopTime) {
  StreamerOptions.StopTimestamp = StopTime;
  MdatWriter->setStopTime(StopTime);
  Executor.sendWork([=]() {
    for (auto &s : Streamers) {
      s->setStopTime(StopTime);
    }
  });
}

void StreamController::pauseStreamers() { StreamersPaused.store(true); }

void StreamController::resumeStreamers() { StreamersPaused.store(false); }

void StreamController::stop() {
  for (auto &Stream : Streamers)
    Stream->stop();
  WriterThread.stop();
  StopNow = true;
}

using duration = std::chrono::system_clock::duration;
bool StreamController::isDoneWriting() {
  auto Now = std::chrono::system_clock::now();
  auto IsDoneWriting =
      HasError or StopNow or
      (!StreamersRemaining.load() and
       StreamerOptions.StopTimestamp != time_point(duration(0)) and
       Now > StreamerOptions.StopTimestamp);
  if (not IsDoneWriting) {
    auto TimeDiffPeriods = (Now - LastFileSizeCalcTime) / FileSizeCalcInterval;
    if (TimeDiffPeriods >= 1) {
      WriterTask->updateApproximateFileSize();
      LastFileSizeCalcTime +=
          FileSizeCalcInterval * int(std::round(TimeDiffPeriods));
    }
  }
  return IsDoneWriting;
}

std::string StreamController::getJobId() const { return WriterTask->jobID(); }

void StreamController::getTopicNames() {
  try {
    auto TopicNames = Kafka::getTopicList(
        StreamerOptions.BrokerSettings.Address, CurrentMetadataTimeOut,
        StreamerOptions.BrokerSettings);
    Executor.sendLowPriorityWork([=]() { initStreams(TopicNames); });
  } catch (MetadataException &E) {
    CurrentMetadataTimeOut *= 2;
    auto &Settings = StreamerOptions.BrokerSettings;
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
  std::string GetTopicsErrorString;
  for (auto &Src : WriterTask->sources()) {
    if (KnownTopicNames.find(Src.topic()) != KnownTopicNames.end()) {
      TopicSrcMap[Src.topic()].push_back(
          {Src.getSrcHash(), Src.getModuleHash(), Src.getWriterPtr(),
           Src.sourcename(), Src.flatbufferID(), Src.writerModuleID(),
           Src.getWriterPtr()->acceptsRepeatedTimestamps()});
    } else {
      GetTopicsErrorString += fmt::format(
          "Unable to set up consumer for source {} on topic {} as this "
          "topic does not exist. ",
          Src.sourcename(), Src.topic());
    }
  }
  if (not GetTopicsErrorString.empty()) {
    LOG_ERROR(GetTopicsErrorString);
    std::lock_guard Guard(ErrorMsgMutex);
    ErrorMessage = GetTopicsErrorString;
    HasError = true;
    return;
  }
  auto CheckStreamersPausedLambda =
      [&StreamersPausedConst = std::as_const(StreamersPaused)]() -> bool {
    return StreamersPausedConst.load(std::memory_order_relaxed);
  };
  for (auto &CItem : TopicSrcMap) {
    auto CStartTime =
        std::chrono::system_clock::time_point(StreamerOptions.StartTimestamp);
    auto CStopTime =
        std::chrono::system_clock::time_point(StreamerOptions.StopTimestamp);
    auto CTopic = std::make_unique<Stream::Topic>(
        StreamerOptions.BrokerSettings, CItem.first, CItem.second,
        &WriterThread, StreamMetricRegistrar, CStartTime,
        StreamerOptions.BeforeStartTime, CStopTime,
        StreamerOptions.AfterStopTime, CheckStreamersPausedLambda);
    CTopic->start();
    Streamers.emplace_back(std::move(CTopic));
  }
  Executor.sendLowPriorityWork([=]() { performPeriodicChecks(); });
}

bool StreamController::hasErrorState() const { return HasError; }

std::string StreamController::errorMessage() {
  std::lock_guard Guard(ErrorMsgMutex);
  return ErrorMessage;
}

void StreamController::performPeriodicChecks() {
  checkIfStreamsAreDone();
  throttleIfWriteQueueIsFull();
  std::this_thread::sleep_for(PeriodicChecksInterval);
  Executor.sendLowPriorityWork([=]() { performPeriodicChecks(); });
}

void StreamController::checkIfStreamsAreDone() {
  try {
    Streamers.erase(
        std::remove_if(Streamers.begin(), Streamers.end(),
                       [](auto const &Elem) { return Elem->isDone(); }),
        Streamers.end());
    if (Streamers.empty()) {
      StreamersRemaining.store(false);
    }
  } catch (std::exception &E) {
    HasError = true;
    std::lock_guard Guard(ErrorMsgMutex);
    ErrorMessage =
        fmt::format("Got stream error. The error message was: {}", E.what());
    stop();
    StreamersRemaining.store(false);
  }
}

void StreamController::throttleIfWriteQueueIsFull() {
  auto QueuedWrites = WriterThread.nrOfWritesQueued();
  if (QueuedWrites > StreamerOptions.MaxQueuedWrites &&
      !StreamersPaused.load()) {
    LOG_DEBUG("Maximum queued writes exceeded (count={}). Pausing consumers...",
              QueuedWrites);
    pauseStreamers();
  } else if (QueuedWrites < QueuedWritesResumeThreshold *
                                StreamerOptions.MaxQueuedWrites &&
             StreamersPaused.load()) {
    LOG_DEBUG("Write queue below maximum (count={}). Resuming consumers...",
              QueuedWrites);
    resumeStreamers();
  }
}

} // namespace FileWriter
