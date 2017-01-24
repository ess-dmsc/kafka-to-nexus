#pragma once
#include <string>
#include <vector>
#include <functional>
#include "TimeDifferenceFromMessage.h"
#include "Source.h"
#include "json.h"

namespace BrightnESS {
namespace FileWriter {


/// %Result of a call to `process_message`.
/// Can be extended later for more detailed reporting.
/// Streamer currently does not accept the return value, therefore currently
/// not used.
class ProcessMessageResult {
public:
static ProcessMessageResult OK();
static ProcessMessageResult ERR();
inline bool is_OK() { return res == 0; }
inline bool is_ERR() { return res == -1; }
private:
char res = -1;
};

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
