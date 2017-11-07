#pragma once
#include "ProcessMessageResult.h"
#include "Source.h"
#include "TimeDifferenceFromMessage.h"
#include "json.h"
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace FileWriter {

class MessageProcessor {
public:
  virtual ProcessMessageResult process_message(char *msg_data,
                                               int msg_size) = 0;
};

/// Represents a sourcename on a topic.
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used
/// for only one sourcename.
class DemuxTopic : public TimeDifferenceFromMessage, public MessageProcessor {
public:
  DemuxTopic(std::string topic);
  std::string const &topic() const;
  /// To be called by FileMaster when a new message is available for this
  /// source. Streamer currently expects void as return, will add return value
  /// in the future.
  ProcessMessageResult process_message(char *msg_data, int msg_size) override;
  /// Implements TimeDifferenceFromMessage.
  DT time_difference_from_message(char *msg_data, int msg_size) override;
  std::unordered_map<std::string, Source> &sources();

  Source &add_source(Source &&source) {
    using std::move;
    auto k = source.sourcename();
    std::pair<std::string, Source> v{k, move(source)};
    return _sources_map.insert(move(v)).first->second;
  }

  std::string to_str() const;
  rapidjson::Document
  to_json(rapidjson::MemoryPoolAllocator<> *_a = nullptr) const;
  uint64_t &stop_time();

private:
  std::string _topic;
  std::unordered_map<std::string, Source> _sources_map;
  uint64_t _stop_time;
};

} // namespace FileWriter
