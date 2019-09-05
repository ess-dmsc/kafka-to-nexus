// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "DemuxTopic.h"
#include "FlatbufferReader.h"
#include "logger.h"
#include <limits>
#include <stdexcept>

namespace FileWriter {

DemuxTopic::DemuxTopic(std::string TopicName) : Topic(std::move(TopicName)) {}

DemuxTopic::~DemuxTopic() { Logger->trace("DemuxTopic destructor"); }

DemuxTopic::DemuxTopic(DemuxTopic &&x) noexcept { swap(*this, x); }

void swap(DemuxTopic &x, DemuxTopic &y) {
  std::swap(x.Topic, y.Topic);
  std::swap(x.TopicSources, y.TopicSources);
}

std::string const &DemuxTopic::topic() const { return Topic; }

ProcessMessageResult
DemuxTopic::process_message(FlatbufferMessage const &Message) {
  Logger->trace("Message received from: {}", Message.getSourceName());
  try {
    auto &CurrentSource = TopicSources.at(Message.getSourceHash());
    auto ProcessingResult = CurrentSource.process_message(Message);
    ++messages_processed;
    return ProcessingResult;
  } catch (std::out_of_range &e) {
    Logger->trace("Source with name \"{}\" and ID \"{}\" is not in list.",
                  Message.getSourceName(), Message.getFlatbufferID());
    ++error_no_source_instance;
  }
  return ProcessMessageResult::ERR;
}

std::unordered_map<FlatbufferMessage::SrcHash, Source> &DemuxTopic::sources() {
  return TopicSources;
}

} // namespace FileWriter
