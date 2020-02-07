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
  /// Initialize with the given TopicName.
  explicit DemuxTopic(std::string TopicName);

  virtual ~DemuxTopic();

  /// \brief Returns the name of the topic that contains the source.
  ///
  /// \return The topic.
  std::string const &topic() const;

  /// \brief Finds the appropriate `Source` for this `Message` and delegates
  /// processing.
  ///
  /// \param Message The flatbuffer message that is to be written to file.
  virtual void process_message(FlatbufferMessage const &Message);

  /// \brief Whether it can handle the specified source.
  bool canHandleSource(FlatbufferMessage::SrcHash SourceHash) const;

  /// Adds a source to topic sources.
  ///
  /// \param source The `Source` to be added.
  ///
  /// \return A reference to the source that has been added.
  void addSource(Source &&source);

  /// Removes a source.
  ///
  /// \param SourceHash Uniquely identifies the source to be removed
  /// \return True if successful, false if key source hash not found
  bool removeSource(FlatbufferMessage::SrcHash SourceHash);

  /// Counts the number of processed messages.
  std::atomic<size_t> messages_processed{0};

private:
  std::string Topic;
  std::unordered_map<FlatbufferMessage::SrcHash, Source> TopicSources;
  SharedLogger Logger = getLogger();
};

} // namespace FileWriter
