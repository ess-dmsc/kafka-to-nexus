#pragma once
#include <string>
#include <vector>
#include <functional>
#include "TimeDifferenceFromMessage.h"
#include "ProcessMessageResult.h"
#include "Source.h"
#include "json.h"

namespace BrightnESS {
namespace FileWriter {


class MessageProcessor {
public:
virtual ProcessMessageResult process_message(char * msg_data, int msg_size) = 0;
};


/// Represents a sourcename on a topic.
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used for only one sourcename.
class DemuxTopic : public TimeDifferenceFromMessage, public MessageProcessor {
public:
DemuxTopic(std::string topic);
std::string const & topic() const;
/// To be called by FileMaster when a new message is available for this source.
/// Streamer currently expects void as return, will add return value in the future.
ProcessMessageResult process_message(char * msg_data, int msg_size);
/// Implements TimeDifferenceFromMessage.
DT time_difference_from_message(char * msg_data, int msg_size);
std::vector<Source> & sources();
template <typename... Args> Source & add_source(Args && ... args) {
	_sources.emplace_back(std::forward<Args>(args)...);
	return _sources.back();
}
std::string to_str() const;
rapidjson::Document to_json(rapidjson::MemoryPoolAllocator<> * _a = nullptr) const;
private:
std::string _topic;
std::vector<Source> _sources;
};

}
}
