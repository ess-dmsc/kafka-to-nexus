#pragma once

#include "DemuxTopic.h"
#include "Status.h"
#include "StreamerOptions.h"

namespace FileWriter {

class SourcesHandler {
public:
  void addSource(const std::string &Source) {
    LOG(Sev::Info, "Add {} to source list", Source);
    if (std::find<std::vector<std::string>::iterator>(
            Sources.begin(), Sources.end(), Source) != Sources.end()) {
      LOG(Sev::Warning, "Topic {} duplicated. Ignore.", Source);
      return;
    }
    Sources.push_back(Source);
  }

  bool removeSource(const std::string &Source) {
    auto Iter(std::find<std::vector<std::string>::iterator>(
        Sources.begin(), Sources.end(), Source));
    if (Iter == Sources.end()) {
      LOG(Sev::Warning, "Can't remove source {}, not in the source list",
          Source);
      return false;
    }
    Sources.erase(Iter);
    LOG(Sev::Info, "Remove source {}", Source);
    return true;
  }

  const size_t numSources() { return Sources.size(); }

  void clear() { Sources.clear(); }

private:
  std::vector<std::string> Sources;
};

class StreamerI {
  using StreamerStatus = Status::StreamerStatus;

public:
  // StreamerI(const std::string &Broker, const std::string &TopicName,
  //           const FileWriter::StreamerOptions &Opts){};

  virtual ProcessMessageResult
  pollAndProcess(FileWriter::DemuxTopic &MessageProcessor) = 0;

  StreamerStatus closeStream() {
    Sources.clear();
    RunStatus = StreamerStatus::HAS_FINISHED;
    return StreamerStatus::HAS_FINISHED;
  };

  const size_t numSources() { return 0; }
  void setSources(std::unordered_map<std::string, Source> &SourceList) {
    for (auto &Src : SourceList) {
      Sources.addSource(Src.first);
    }
  }
  bool removeSource(const std::string &Source) {
    return Sources.removeSource(Source);
  }

  //----------------------------------------------------------------------------
  /// @brief      Returns the status of the Streamer. See "Error.h"
  ///
  /// @return     The current status
  ///
  StreamerStatus &runStatus() { return RunStatus; }

  /// Return all the informations about the messages consumed
  Status::MessageInfo &messageInfo() { return MessageInfo; }

private:
  StreamerStatus RunStatus{StreamerStatus::NOT_INITIALIZED};
  Status::MessageInfo MessageInfo;
  SourcesHandler Sources;
};

template <class StreamerType>
std::unique_ptr<StreamerType> createStream(const std::string &Broker,
                                           DemuxTopic &Demux,
                                           const StreamerOptions &Opts) {
  std::unique_ptr<StreamerType> Streamer =
      std::make_unique<StreamerType>(Broker, Demux.topic(), Opts);
  Streamer->setSources(Demux.sources());
  return Streamer;
};
}
