#pragma once

#include "DemuxTopic.h"
#include "Status.h"
#include "StreamerOptions.h"

namespace FileWriter {

class SourcesHandler {
public:
  void addSource(const std::string &Source) {
    LOG(Sev::Info, "Add {} to source list", Source);
    if (isPresent(Source)) {
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

  bool isPresent(const std::string &Source) {
    return std::find<std::vector<std::string>::iterator>(
               Sources.begin(), Sources.end(), Source) != Sources.end();
  }

  const size_t numSources() { return Sources.size(); }

  void clear() { Sources.clear(); }

private:
  std::vector<std::string> Sources;
};

class IStreamer {
  using StreamerStatus = Status::StreamerStatus;

public:
  virtual ~IStreamer() = default;

  virtual std::string getName() const { return "IStreamer"; }

  virtual ProcessMessageResult
  pollAndProcess(FileWriter::DemuxTopic &MessageProcessor) = 0;

  /// \brief Disconnect the kafka consumer and destroy the TopicPartition vector. Make
  /// sure that the Streamer status is StreamerErrorCode::has_finished
  StreamerStatus closeStream() {
    Sources.clear();
    RunStatus = StreamerStatus::HAS_FINISHED;
    return StreamerStatus::HAS_FINISHED;
  };

  /// \brief Return the number of different sources whose last message is
  /// not older than the stop time.
  ///
  /// \return The number of sources.
  const size_t numSources() { return 0; }
  void setSources(std::unordered_map<std::string, Source> &SourceList) {
    for (auto &Src : SourceList) {
      Sources.addSource(Src.first);
    }
  }

  /// \brief Removes the source from the sources list.
  ///
  /// \param SourceName The name of the source to be removed.
  /// \return True if success, else false (e.g. the source is not in the
  /// list).
  bool removeSource(const std::string &Source) {
    return Sources.removeSource(Source);
  }

  /// \brief Returns the status of the Streamer. See "Error.h".
  ///
  /// \return The current status.
  StreamerStatus &runStatus() { return RunStatus; }

  /// Return all the informations about the messages consumed.
  Status::MessageInfo &messageInfo() { return MessageInfo; }

protected:
  StreamerStatus RunStatus{StreamerStatus::NOT_INITIALIZED};
  Status::MessageInfo MessageInfo;
  SourcesHandler Sources;
};
}
