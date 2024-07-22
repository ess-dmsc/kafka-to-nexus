#include "StreamController.h"

#include "FileWriterTask.h"
#include "HDFOperations.h"
#include "Kafka/MetaDataQuery.h"
#include "Kafka/MetadataException.h"
#include "Stream/Partition.h"
#include "TimeUtility.h"
#include "helper.h"
#include "logger.h"
#include <utility>

namespace FileWriter {

using namespace std::chrono_literals;

StreamController::StreamController(
    std::unique_ptr<FileWriterTask> FileWriterTask,
    std::unique_ptr<WriterModule::mdat::mdat_Writer> mdatWriter,
    FileWriter::StreamerOptions const &Settings, Metrics::IRegistrar *Registrar,
    MetaData::TrackerPtr Tracker,
    std::shared_ptr<Kafka::MetadataEnquirer> metadata_enquirer,
    std::shared_ptr<Kafka::ConsumerFactoryInterface> consumer_factory)
    : CurrentMetadataTimeOut(Settings.BrokerSettings.MinMetadataTimeout),
      WriterTask(std::move(FileWriterTask)), MdatWriter(std::move(mdatWriter)),
      StreamMetricRegistrar(Registrar->getNewRegistrar("")),
      WriterThread([this]() { WriterTask->flushDataToFile(); },
                   Settings.DataFlushInterval,
                   Registrar->getNewRegistrar("stream.writer")),
      StreamerOptions(Settings), MetaDataTracker(std::move(Tracker)),
      _metadata_enquirer(std::move(metadata_enquirer)),
      _consumer_factory(std::move(consumer_factory)),
      _worker_thread(&StreamController::process, this) {
  MdatWriter->set_start_time(StreamerOptions.StartTimestamp);
  MdatWriter->set_stop_time(StreamerOptions.StopTimestamp);
}

StreamController::~StreamController() { stop(); }

void StreamController::process() {
  setThreadName("stream_controller");
  Status status{Status::Initialising};

  while (_run_thread.load()) {
    switch (status) {
    case Status::Initialising: {
      status = initialise_streams();
      break;
    }
    case Status::Running: {
      performPeriodicChecks();
      break;
    }
    case Status::Error: {
      _run_thread.store(false);
      break;
    }
    default:
      assert("Unexpected StreamController status");
    }
  }
  Logger::Info("StreamController threaded process exiting");
}

StreamController::Status StreamController::initialise_streams() {
  auto topic_names = getTopicNames();
  if (topic_names.empty()) {
    return Status::Initialising;
  }
  auto success = initStreams(topic_names);
  if (success) {
    return Status::Running;
  } else {
    return Status::Error;
  }
}

void StreamController::setStopTime(time_point const &StopTime) {
  StreamerOptions.StopTimestamp = StopTime;
  MdatWriter->set_stop_time(StopTime);
  for (auto &s : Streamers) {
    s->setStopTime(StopTime);
  }
}

void StreamController::pauseStreamers() { StreamersPaused.store(true); }

void StreamController::resumeStreamers() { StreamersPaused.store(false); }

void StreamController::stop() {
  MdatWriter->write_metadata(*WriterTask);
  Logger::Info("Stopped StreamController for file with id : {}",
               StreamController::getJobId());
  for (auto &Stream : Streamers)
    Stream->stop();
  WriterThread.stop();
  StopNow = true;
  _run_thread.store(false);
  if (_worker_thread.joinable()) {
    _worker_thread.join();
  }
}

using duration = std::chrono::system_clock::duration;
bool StreamController::isDoneWriting() {
  auto Now = std::chrono::system_clock::now();
  auto IsDoneWriting =
      HasError or StopNow or
      (!StreamersRemaining.load() and
       StreamerOptions.StopTimestamp != time_point(duration(0)) and
       Now > StreamerOptions.StopTimestamp);
  if (!IsDoneWriting) {
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

std::set<std::string> StreamController::getTopicNames() {
  try {
    return _metadata_enquirer->getTopicList(
        StreamerOptions.BrokerSettings.Address, CurrentMetadataTimeOut,
        StreamerOptions.BrokerSettings);
  } catch (MetadataException &E) {
    CurrentMetadataTimeOut *= 2;
    auto &Settings = StreamerOptions.BrokerSettings;
    if (CurrentMetadataTimeOut > Settings.MaxMetadataTimeout) {
      CurrentMetadataTimeOut = Settings.MaxMetadataTimeout;
    }
    Logger::Info("Meta data call for retrieving topic names from the broker "
                 "failed. Re-trying with a timeout of {} ms.",
                 std::chrono::duration_cast<std::chrono::milliseconds>(
                     CurrentMetadataTimeOut)
                     .count());
    return {};
  }
}

bool StreamController::initStreams(std::set<std::string> known_topic_names) {
  std::map<std::string, Stream::SrcToDst> topic_src_map;
  std::string errors_collector;
  for (auto &src : WriterTask->sources()) {
    if (known_topic_names.find(src.topic()) != known_topic_names.end()) {
      topic_src_map[src.topic()].push_back(
          {src.getSrcHash(), src.getModuleHash(), src.getWriterPtr(),
           src.sourcename(), src.flatbufferID(), src.writerModuleID(),
           src.getWriterPtr()->acceptsRepeatedTimestamps()});
    } else {
      errors_collector += fmt::format(
          "Unable to set up consumer for source {} on topic {} as this "
          "topic does not exist. ",
          src.sourcename(), src.topic());
    }
  }
  if (!errors_collector.empty()) {
    Logger::Error(errors_collector);
    std::lock_guard guard(ErrorMsgMutex);
    ErrorMessage = errors_collector;
    HasError = true;
    return false;
  }
  auto check_streamers_paused_func =
      [&StreamersPausedConst = std::as_const(StreamersPaused)]() -> bool {
    return StreamersPausedConst.load(std::memory_order_relaxed);
  };
  for (auto &[topic_name, source_map] : topic_src_map) {
    auto start_time =
        std::chrono::system_clock::time_point(StreamerOptions.StartTimestamp);
    auto stop_time =
        std::chrono::system_clock::time_point(StreamerOptions.StopTimestamp);
    auto topic = std::make_unique<Stream::Topic>(
        StreamerOptions.BrokerSettings, topic_name, source_map, &WriterThread,
        StreamMetricRegistrar.get(), start_time,
        StreamerOptions.BeforeStartTime, stop_time,
        StreamerOptions.AfterStopTime, check_streamers_paused_func,
        _metadata_enquirer, _consumer_factory);
    topic->start();
    Streamers.emplace_back(std::move(topic));
  }
  return true;
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
    ErrorMessage = fmt::format(
        "Got stream error so stopping file writing. The error message was: {}",
        E.what());
    stop();
    StreamersRemaining.store(false);
  }
}

void StreamController::throttleIfWriteQueueIsFull() {
  auto QueuedWrites = WriterThread.nrOfWritesQueued();
  if (QueuedWrites > StreamerOptions.MaxQueuedWrites &&
      !StreamersPaused.load()) {
    Logger::Debug(
        "Maximum queued writes exceeded (count={}). Pausing consumers...",
        QueuedWrites);
    pauseStreamers();
  } else if (QueuedWrites < QueuedWritesResumeThreshold *
                                StreamerOptions.MaxQueuedWrites &&
             StreamersPaused.load()) {
    Logger::Debug("Write queue below maximum (count={}). Resuming consumers...",
                  QueuedWrites);
    resumeStreamers();
  }
}

} // namespace FileWriter
