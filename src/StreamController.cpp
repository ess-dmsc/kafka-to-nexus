#include "StreamController.h"
#include "FileWriterTask.h"
#include "HDFOperations.h"
#include "Kafka/ConsumerFactory.h"
#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"
#include "MultiVector.h"
#include "Stream/Partition.h"
#include "TimeUtility.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {
StreamController::StreamController(
    std::unique_ptr<FileWriterTask> FileWriterTask,
    FileWriter::StreamerOptions const &Settings,
    Metrics::Registrar const &Registrar, MetaData::TrackerPtr const &Tracker)

    : WriterTask(std::move(FileWriterTask)), StreamMetricRegistrar(Registrar),
      WriterThread([this]() { WriterTask->flushDataToFile(); },
                   Settings.DataFlushInterval,
                   Registrar.getNewRegistrar("stream")),
      StreamerOptions(Settings), MetaDataTracker(Tracker) {
  writeStartTime();
  Executor.sendLowPriorityWork([=]() {
    CurrentMetadataTimeOut = Settings.BrokerSettings.MinMetadataTimeout;
    getTopicNames();
  });
}

StreamController::~StreamController() {
  stop();
  writeStopTime();
  LOG_INFO("Stopped StreamController for file with id : {}",
           StreamController::getJobId());
}

void StreamController::writeStartTime() {
  writeTimePointAsIso8601String("/entry", "start_time",
                                StreamerOptions.StartTimestamp);
}

void StreamController::writeStopTime() {
  writeTimePointAsIso8601String("/entry", "stop_time",
                                StreamerOptions.StopTimestamp);
}

void StreamController::writeTimePointAsIso8601String(std::string const &Path,
                                                     std::string const &Name,
                                                     time_point const &Value) {
  try {
    auto StringVec = MultiVector<std::string>{{1}};
    StringVec.at({0}) = toUTCDateTime(Value);
    auto Group = hdf5::node::get_group(WriterTask->hdfGroup(), Path);
    HDFOperations::writeStringDataset(Group, Name, StringVec);
  } catch (std::exception &Error) {
    LOG_ERROR("Failed to write time-point as ISO8601: {}", Error.what());
  }
}

void StreamController::setStopTime(time_point const &StopTime) {
  StreamerOptions.StopTimestamp = StopTime;
  Executor.sendWork([=]() {
    for (auto &s : Streamers) {
      s->setStopTime(StopTime);
    }
  });
}

void StreamController::pauseStreamers() {
  for (auto &Stream : Streamers) {
    Stream->pause();
  }
  // We set the mutex last. Pause operations are idempotent, so we worry more
  // about preventing a concurrent call to resumeStreamers() than a concurrent
  // call to pauseStreamers()
  StreamersPaused.store(true);
}

void StreamController::resumeStreamers() {
  for (auto &Stream : Streamers) {
    Stream->resume();
  }
  // We set the mutex last. Resume operations are idempotent, so we worry more
  // about preventing a concurrent call to pauseStreamers() than a concurrent
  // call to resumeStreamers()
  StreamersPaused.store(false);
}

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
  for (auto &CItem : TopicSrcMap) {
    auto CStartTime =
        std::chrono::system_clock::time_point(StreamerOptions.StartTimestamp);
    auto CStopTime =
        std::chrono::system_clock::time_point(StreamerOptions.StopTimestamp);
    auto CTopic = std::make_unique<Stream::Topic>(
        StreamerOptions.BrokerSettings, CItem.first, CItem.second,
        &WriterThread, StreamMetricRegistrar, CStartTime,
        StreamerOptions.BeforeStartTime, CStopTime,
        StreamerOptions.AfterStopTime);
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
