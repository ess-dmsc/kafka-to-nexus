#include "DemuxTopic.h"
#include "FlatbufferReader.h"
#include "logger.h"
#include <limits>
#include <stdexcept>

namespace FileWriter {

ProcessMessageResult ProcessMessageResult::OK(int64_t ts) {
  if (ts < 0) {
    throw std::runtime_error("Invalid timestamp");
  }
  ProcessMessageResult ret;
  ret._ts = ts;
  return ret;
}

ProcessMessageResult ProcessMessageResult::ERR() {
  ProcessMessageResult ret;
  ret._ts = -1;
  return ret;
}

ProcessMessageResult ProcessMessageResult::ALL_SOURCES_FULL() {
  ProcessMessageResult ret;
  ret._ts = -2;
  return ret;
}

ProcessMessageResult ProcessMessageResult::STOP() {
  ProcessMessageResult ret;
  ret._ts = -3;
  return ret;
}

DemuxTopic::DemuxTopic(std::string topic)
    : _topic(topic), _stop_time(std::numeric_limits<uint64_t>::max()) {}

DemuxTopic::~DemuxTopic() { LOG(Sev::Debug, "dtor"); }

DemuxTopic::DemuxTopic(DemuxTopic &&x) { swap(*this, x); }

void swap(DemuxTopic &x, DemuxTopic &y) {
  using std::swap;
  swap(x._topic, y._topic);
  swap(x._sources_map, y._sources_map);
  swap(x._stop_time, y._stop_time);
}

DemuxTopic::DT DemuxTopic::time_difference_from_message(Msg const &msg) {
  auto &reader = FlatbufferReaderRegistry::find(msg);
  if (!reader) {
    LOG(Sev::Error, "ERROR unknown schema id?");
    return DT::ERR();
  }
  auto srcn = reader->source_name(msg);
  return DT(srcn, reader->timestamp(msg));
}

std::string const &DemuxTopic::topic() const { return _topic; }

ProcessMessageResult DemuxTopic::process_message(Msg &&msg) {
  if (msg.size() < 8) {
    LOG(Sev::Error, "too small message");
    ++error_message_too_small;
    return ProcessMessageResult::ERR();
  }
  auto &reader = FlatbufferReaderRegistry::find(msg);
  if (!reader) {
    LOG(Sev::Debug, "no reader");
    ++error_no_flatbuffer_reader;
    return ProcessMessageResult::ERR();
  }
  auto srcn = reader->source_name(msg);
  LOG(Sev::Debug, "Msg is for source_name: {}", srcn);
  try {
    auto &s = _sources_map.at(srcn);
    auto ret = s.process_message(msg);
    ++messages_processed;
    return ret;
  } catch (std::out_of_range &e) {
    ++error_no_source_instance;
  }
  return ProcessMessageResult::ERR();
}

std::unordered_map<std::string, Source> &DemuxTopic::sources() {
  return _sources_map;
}

std::string DemuxTopic::to_str() const { return to_json().dump(); }

nlohmann::json DemuxTopic::to_json() const {
  auto JSON = nlohmann::json::object();
  JSON["__KLASS__"] = "DemuxTopic";
  JSON["topic"] = topic();
  auto Sources = nlohmann::json::array();
  for (auto it = _sources_map.cbegin(); it != _sources_map.cend(); ++it) {
    Sources.push_back(it->second.to_json());
  }
  JSON["sources"] = Sources;
  return JSON;
}

std::chrono::milliseconds &DemuxTopic::stop_time() { return _stop_time; }

} // namespace FileWriter
