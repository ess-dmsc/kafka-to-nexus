#include "StreamMaster.h"
#include "Streamer.h"

FileWriter::StreamMaster::StreamMaster(
    const std::string &Broker, std::unique_ptr<FileWriterTask> FileWriterTask,
    const MainOpt &Options, std::unique_ptr<IStreamerFactory> StreamerFactory)
    : Demuxers(FileWriterTask->demuxers()),
      ProducerTopic(FileWriterTask->getStatusProducer()),
      WriterTask(std::move(FileWriterTask)),
      TopicWriteDuration{Options.topic_write_duration},
      ServiceId{Options.service_id} {

  for (auto &Demux : Demuxers) {
    try {
      Streamers.emplace(std::piecewise_construct,
                        std::forward_as_tuple(Demux.topic()),
                        std::forward_as_tuple(StreamerFactory->create(
                            Broker, Demux, Options.StreamerConfiguration)));
    } catch (std::exception &E) {
      RunStatus = StreamMasterError::STREAMER_ERROR();
      LOG(Sev::Critical, "{}", E.what());
      logEvent(ProducerTopic, StatusCode::Error, ServiceId,
               WriterTask->jobID(), E.what());
    }
  }
  NumStreamers = Streamers.size();
  ReportMessageDelay =
      std::chrono::milliseconds{Options.status_master_interval};
}

FileWriter::StreamMaster::~StreamMaster() {
  Stop = true;
  if (WriteThread.joinable()) {
    WriteThread.join();
  }
  if (ReportThread.joinable()) {
    ReportThread.join();
  }
  LOG(Sev::Info, "Stop StreamMaster for file with id : {}, ready to be removed",
      getJobId());
}

bool FileWriter::StreamMaster::setStopTime(
    const std::chrono::milliseconds &StopTime) {
  for (auto &s : Streamers) {
    try {
      //        s.second->getOptions().StopTimestamp = StopTime;
    } catch (std::exception &E) {
      LOG(Sev::Error, "Unable to set stop time in topic {}: {}", s.first,
          E.what());
    }
  }
  return true;
}

bool FileWriter::StreamMaster::start() {
  runReport(ReportMessageDelay);
  if (NumStreamers == 0) {
    Stop = true;
    stopImplemented();
    return WriteThread.joinable();
  }
  LOG(Sev::Info, "StreamMaster: start");
  Stop = false;

  if (!WriteThread.joinable()) {
    WriteThread = std::thread([&] { this->run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return WriteThread.joinable();
}

bool FileWriter::StreamMaster::stop() {
  LOG(Sev::Info, "StreamMaster: stop");
  Stop = true;
  return !(WriteThread.joinable() || ReportThread.joinable());
}

const FileWriter::StreamMaster::StreamMasterError
FileWriter::StreamMaster::status() {
  for (auto &s : Streamers) {
    try {
      if (s.second->runStatus() >= StreamerStatus::IS_CONNECTED) {
        return StreamMasterError::STREAMER_ERROR();
      }
    } catch (std::exception &E) {
      LOG(Sev::Error, "Error, invalid stream {}.", s.first);
      return StreamMasterError::STREAMER_ERROR();
    }
  }
  return RunStatus.load();
}

FileWriter::StreamMaster::StreamMasterError
FileWriter::StreamMaster::processStreamResult(DemuxTopic &Demux) {
  auto ProcessStartTime = std::chrono::system_clock::now();
  FileWriter::ProcessMessageResult ProcessResult;

  // process stream for at most TopicWriteDuration milliseconds
  while ((std::chrono::system_clock::now() - ProcessStartTime) <
         TopicWriteDuration) {
    if (Stop) {
      return StreamMasterError::HAS_FINISHED();
    }

    // if Streamer throws the stream is closed, but the writing continues
    try {
      ProcessResult = Streamers[Demux.topic()]->pollAndProcess(Demux);
    } catch (std::exception &E) {
      LOG(Sev::Error, "Stream closed due to stream error: {}", E.what());
      logEvent(ProducerTopic, StatusCode::Error, ServiceId,
               WriterTask->jobID(), E.what());
      closeStream(Demux.topic());
      return StreamMasterError::STREAMER_ERROR();
    }
    // decreases the count of sources in the stream, eventually closes the
    // stream
    if (ProcessResult == ProcessMessageResult::STOP) {
      if (Streamers[Demux.topic()]->numSources() == 0) {
        return closeStream(Demux.topic());
      }
      return StreamMasterError::RUNNING();
    }
    // if there's any error in the messages logs it
    if (ProcessResult == ProcessMessageResult::ERR) {
      LOG(Sev::Error, "Error in topic \"{}\" : {}", Demux.topic(),
          Err2Str(Streamers[Demux.topic()]->runStatus()));
      return StreamMasterError::STREAMER_ERROR();
    }
  }
  return StreamMasterError::RUNNING();
}

void FileWriter::StreamMaster::run() {
  using namespace std::chrono;
  RunStatus = StreamMasterError::RUNNING();
  while (!Stop && NumStreamers > 0 && Demuxers.size() > 0) {

    for (auto &Demux : Demuxers) {

      // If the stream is active process the messages
      StreamMasterError ProcessResult = processStreamResult(Demux);
      if (ProcessResult == StreamMasterError::HAS_FINISHED()) {
        continue;
      }
      if (ProcessResult == StreamMasterError::STREAMER_ERROR()) {
        continue;
      }
    }
  }
  RunStatus = StreamMasterError::HAS_FINISHED();
  stopImplemented();
}

FileWriter::StreamMaster::StreamMasterError
FileWriter::StreamMaster::closeStream(const std::string &TopicName) {
  LOG(Sev::Debug, "All sources in Stream have expired, close connection");
  Streamers[TopicName]->runStatus() = Status::StreamerStatus::HAS_FINISHED;
  Streamers[TopicName]->closeStream();
  NumStreamers--;
  if (NumStreamers != 0) {
    return StreamMasterError::RUNNING();
  }
  Stop = true;
  return StreamMasterError::HAS_FINISHED();
}

void FileWriter::StreamMaster::stopImplemented() {
  if (ReportThread.joinable()) {
    ReportThread.join();
  }
  for (auto &s : Streamers) {
    LOG(Sev::Info, "Shut down {}", s.first);
    auto v = s.second->closeStream();
    if (v == StreamerStatus::HAS_FINISHED) {
      LOG(Sev::Warning, "Error while stopping {} : {}", s.first,
          Status::Err2Str(v));
    } else {
      LOG(Sev::Info, "\t...done");
    }
  }
  Streamers.clear();
  RunStatus = StreamMasterError::IS_REMOVABLE();
  LOG(Sev::Info, "RunStatus:  {}", Err2Str(RunStatus));
}

void FileWriter::StreamMaster::runReport(
    const std::chrono::milliseconds &ReportMs) {
  if (NumStreamers != 0) {
    if (!ReportThread.joinable()) {
      ReportPtr.reset(
          new Report(ProducerTopic, WriterTask->jobID(), ReportMs));
      ReportThread =
          std::thread([&] { ReportPtr->report(Streamers, Stop, RunStatus); });
    } else {
      LOG(Sev::Debug, "Status report already started, nothing to do");
    }
  }
}
