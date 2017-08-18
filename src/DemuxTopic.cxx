#include "DemuxTopic.h"
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

DemuxTopic::DT DemuxTopic::time_difference_from_message(char *msg_data,
                                                        int msg_size) {
  Msg msg{msg_data, size_t(msg_size)};
  std::string _tmp_dummy;
  auto reader = FBSchemaReader::create(msg);
  if (!reader) {
    LOG(4, "ERROR unknown schema id?");
    return DT::ERR();
  }
  auto srcn = reader->sourcename(msg);
  return DT(srcn, reader->ts(msg));
}

std::string const &DemuxTopic::topic() const { return _topic; }

ProcessMessageResult DemuxTopic::process_message(char *msg_data, int msg_size) {
  Msg msg{msg_data, size_t(msg_size)};
  auto reader = FBSchemaReader::create(msg);
  if (!reader) {
    return ProcessMessageResult::ERR();
  }
  if (reader->ts(msg) > _stop_time) {
    LOG(7, "reader->ts(msg) {} > _stop_time {}", reader->ts(msg), _stop_time);
    return ProcessMessageResult::STOP();
  }
  auto srcn = reader->sourcename(msg);
  LOG(7, "Msg is for sourcename: {}", srcn);
  try {
    auto &s = _sources_map.at(srcn);
    auto ret = s.process_message(msg);
    if (ret.ts() < 0) {
      return ProcessMessageResult::ERR();
    }
    return ProcessMessageResult::OK(ret.ts());
  } catch (std::out_of_range &e) {
  }
  return ProcessMessageResult::ERR();
}

std::unordered_map<std::string, Source> &DemuxTopic::sources() {
  return _sources_map;
}

std::string DemuxTopic::to_str() const { return json_to_string(to_json()); }

rapidjson::Document
DemuxTopic::to_json(rapidjson::MemoryPoolAllocator<> *_a) const {
  using namespace rapidjson;
  Document jd;
  if (_a)
    jd = Document(_a);
  auto &a = jd.GetAllocator();
  jd.SetObject();
  auto &v = jd;
  v.AddMember("__KLASS__", "DemuxTopic", a);
  v.AddMember("topic", Value(topic().data(), a), a);
  Value kl;
  kl.SetArray();
  for (auto it = _sources_map.cbegin(); it != _sources_map.cend(); ++it) {
    kl.PushBack(it->second.to_json(&a), a);
  }
  v.AddMember("sources", kl, a);
  return jd;
}

uint64_t &DemuxTopic::stop_time() { return _stop_time; }

} // namespace FileWriter
