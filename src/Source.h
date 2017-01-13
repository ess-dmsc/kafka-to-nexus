#pragma once
#include <string>
#include "TimeDifferenceFromMessage.h"

namespace BrightnESS {
namespace FileWriter {

class Result {
public:
bool is_OK();
bool is_ERR();
};

/// Represents a sourcename on a topic.
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used for only one sourcename.
class Source {
public:
Source(std::string topic, std::string source);
std::string const & topic();
std::string const & source();
Result process_message(void * msg_data, int msg_size);
private:
std::string _topic;
std::string _source;
};

}
}
