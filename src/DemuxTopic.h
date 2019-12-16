// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once
#include "ProcessMessageResult.h"
#include "Source.h"
#include "logger.h"
#include <string>
#include <unordered_map>

namespace FileWriter {

class MessageProcessingException : public std::runtime_error {
public:
  explicit MessageProcessingException(const std::string &ErrorMessage)
      : std::runtime_error(ErrorMessage) {}
};

/// \brief Used to keep track of file writing modules on one topic and call the
/// correct module based on sourcename.
class DemuxTopic {
public:
  DemuxTopic() = default;

  /// Initialize with the given TopicName.
  explicit DemuxTopic(std::string TopicName);

  /// Move constructor.
  DemuxTopic(DemuxTopic &&x) noexcept;

  virtual ~DemuxTopic();

  /// \brief Returns the name of the topic that contains the source.
  ///
  /// \return The topic.
  std::string const &topic() const;

  /// \brief Finds the appropriate `Source` for this `Message` and delegates
  /// processing.
  ///
  /// \param Message The flatbuffer message that is to be written to file.
  ///
  /// \return A status message indicating if the write was successful.
  virtual void
  process_message(FlatbufferMessage const &Message);

  /// \brief Gets list of sources handled on this topic.
  ///
  /// \return Unordered map of sources
  std::unordered_map<FlatbufferMessage::SrcHash, Source> &sources();

  /// Adds a source to topic sources.
  ///
  /// \param source The `Source` to be added.
  ///
  /// \return A reference to the source that has been added.
  Source &add_source(Source &&source) {
    auto k = source.getHash();
    std::pair<FlatbufferMessage::SrcHash, Source> v{k, std::move(source)};
    return TopicSources.insert(std::move(v)).first->second;
  }

  /// Removes a source
  /// for example when the last message before the stop time has been consumed
  ///
  /// \param SourceHash Uniquely identifies the source to be removed
  /// \return True if successful, false if key source hash not found
  bool removeSource(FlatbufferMessage::SrcHash SourceHash) {
    return static_cast<bool>(TopicSources.erase(SourceHash));
  }

  /// Counts the number of processed message.
  std::atomic<size_t> messages_processed{0};

  /// \brief Counts the number of times when a received message is so small that
  /// it can not be a valid flatbuffer.
  std::atomic<size_t> error_message_too_small{0};

  /// \brief Counts the number of times when we can not find a reader for this
  /// type of flatbuffer.
  std::atomic<size_t> error_no_flatbuffer_reader{0};

  /// \brief Counts the number of times when we can not find a source instance
  /// for the source_name mentioned in the the flatbuffer message.
  std::atomic<size_t> error_no_source_instance{0};

private:
  std::string Topic;
  std::unordered_map<FlatbufferMessage::SrcHash, Source> TopicSources;
  friend void swap(DemuxTopic &x, DemuxTopic &y);
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
