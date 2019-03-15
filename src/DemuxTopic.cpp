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
    auto &CurrentSource = TopicSources.at(Message.getSourceName());
    auto ProcessingResult = CurrentSource.process_message(Message);
    ++messages_processed;
    return ProcessingResult;
  } catch (std::out_of_range &e) {
    Logger->trace("Source with name \"{}\" is not in list.",
                  Message.getSourceName());
    ++error_no_source_instance;
  }
  return ProcessMessageResult::ERR;
}

std::unordered_map<std::string, Source> &DemuxTopic::sources() {
  return TopicSources;
}

} // namespace FileWriter
