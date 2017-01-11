#pragma once
#include <string>
#include "TimeDifferenceFromMessage.h"

namespace BrightnESS {
namespace FileWriter {

/// Represents a sourcename on a topic.
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used for only one sourcename.
class Source : public TimeDifferenceFromMessage {
public:
Source(std::string topic, std::string source);
std::string const & topic();
std::string const & source();
int64_t time_difference_from_message(void * msg_data, int msg_size);
private:
std::string _topic;
std::string _source;
};

}
}
