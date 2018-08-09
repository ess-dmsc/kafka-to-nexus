#pragma once
#include "MessageTimestamp.h"
#include "ProcessMessageResult.h"
#include "Source.h"
#include "json.h"
#include <chrono>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace FileWriter {

/// Used to keep track of file writing modules on one topic and call the correct
/// module based on sourcename
class DemuxTopic {
public:
  /// Initialize with the given topic name
  DemuxTopic(std::string TopicName);

  /// Move constructor
  DemuxTopic(DemuxTopic &&x);

  /// Virtual destructor apparently because some mock derives from this?
  /// \todo Should probably use proper interface (eg `DemuxTopicI`)
  virtual ~DemuxTopic();

  /// Returns the name of the topic that contains the source
  /// \return The topic
  std::string const &topic() const;

  /// Finds the appropriate `Source` for this `Message` and delegates
  /// processing.
  /// Called typically from `Streamer`.
  /// \param[in] Message The flatbuffer message that is to be written to file.
  /// \return A status message indicating if the write was successfull.
  virtual ProcessMessageResult
  process_message(FlatbufferMessage const &Message);

  /// \return The list of sources handled on this topic.
  std::unordered_map<std::string, Source> &sources();

  /// Adds a source.
  ///
  /// \param[in]  source  The `Source` to be added.
  ///
  /// \return A reference to the source that has been added to the source
  Source &add_source(Source &&source) {
    auto k = source.sourcename();
    std::pair<std::string, Source> v{k, std::move(source)};
    return TopicSources.insert(std::move(v)).first->second;
  }

  /// Counts the number of processed message.
  std::atomic<size_t> messages_processed{0};
  /// Counts the number of times when a received message is so small that it
  /// can not be a valid flatbuffer.
  std::atomic<size_t> error_message_too_small{0};
  /// Counts the number of times when we can not find a reader for this type of
  /// flatbuffer.
  std::atomic<size_t> error_no_flatbuffer_reader{0};
  /// Counts the number of times when we can not find a source instance for the
  /// source_name mentioned in the the flatbuffer message.
  std::atomic<size_t> error_no_source_instance{0};

private:
  std::string Topic;
  std::unordered_map<std::string, Source> TopicSources;
  friend void swap(DemuxTopic &x, DemuxTopic &y);
};

} // namespace FileWriter
