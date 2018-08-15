#include "DemuxTopic.h"
#include "FlatbufferReader.h"
#include "logger.h"
#include <limits>
#include <stdexcept>

namespace FileWriter {

DemuxTopic::DemuxTopic(std::string TopicName) : Topic(TopicName) {}

DemuxTopic::~DemuxTopic() {
  // Empty dtor kept to simplify merge in a later PR which will add code here.
}

DemuxTopic::DemuxTopic(DemuxTopic &&x) { swap(*this, x); }

void swap(DemuxTopic &x, DemuxTopic &y) {
  using std::swap;
  swap(x.Topic, y.Topic);
  swap(x.TopicSources, y.TopicSources);
}

std::string const &DemuxTopic::topic() const { return Topic; }

ProcessMessageResult
DemuxTopic::process_message(FlatbufferMessage const &Message) {
  LOG(Sev::Debug, "Message received from: {}", Message.getSourceName());
  try {
    auto &CurrentSource = TopicSources.at(Message.getSourceName());
    auto ProcessingResult = CurrentSource.process_message(Message);
    ++messages_processed;
    return ProcessingResult;
  } catch (std::out_of_range &e) {
    LOG(Sev::Debug, "Source with name \"{}\" is not in list.",
        Message.getSourceName());
    ++error_no_source_instance;
  }
  return ProcessMessageResult::ERR;
}

std::unordered_map<std::string, Source> &DemuxTopic::sources() {
  return TopicSources;
}

} // namespace FileWriter
