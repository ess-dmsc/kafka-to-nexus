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

MessageTimestamp getMessageTime(Msg const &Msg) {
  try {
    auto &Reader = FlatbufferReaderRegistry::find(Msg);
    if (!Reader) {
      throw std::runtime_error(
          "Unable to locate reader with the correct key in the registry.");
    }
    auto Name = Reader->source_name(Msg);
    return MessageTimestamp(Name, Reader->timestamp(Msg));
  } catch (std::runtime_error &E) {
    LOG(Sev::Error, "Problem when getting message time: {}", E.what());
    throw E;
  }
  return MessageTimestamp();
}

std::string const &DemuxTopic::topic() const { return Topic; }

ProcessMessageResult DemuxTopic::process_message(Msg &&Msg) {
  std::string CurrentSourceName;
  try {
    auto &Reader = FlatbufferReaderRegistry::find(Msg);
    if (!Reader) {
      LOG(Sev::Debug,
          "Unable to locate reader with the correct key in the registry.");
      ++error_no_flatbuffer_reader;
      return ProcessMessageResult::ERR;
    }
    CurrentSourceName = Reader->source_name(Msg);
  } catch (std::runtime_error &E) {
    LOG(Sev::Error, "{}", E.what());
    ++error_message_too_small;
    return ProcessMessageResult::ERR;
  }
  LOG(Sev::Debug, "Message received from: {}", CurrentSourceName);
  try {
    auto &CurrentSource = TopicSources.at(CurrentSourceName);
    auto ProcessingResult = CurrentSource.process_message(Msg);
    ++messages_processed;
    return ProcessingResult;
  } catch (std::out_of_range &e) {
    LOG(Sev::Debug, "Source with name \"{}\" is not in list.",
        CurrentSourceName);
    ++error_no_source_instance;
  }
  return ProcessMessageResult::ERR;
}

std::unordered_map<std::string, Source> &DemuxTopic::sources() {
  return TopicSources;
}

} // namespace FileWriter
