#pragma once
#include <string>
#include "TimeDifferenceFromMessage.h"

class Test___FileWriterTask___Create01;

namespace BrightnESS {
namespace FileWriter {

class SourceFactory_by_FileWriterTask;

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
Source(Source &&);
std::string const & topic();
std::string const & source();
Result process_message(void * msg_data, int msg_size);
private:
Source(std::string topic, std::string source);

std::string _topic;
std::string _source;

friend class SourceFactory_by_FileWriterTask;
friend class ::Test___FileWriterTask___Create01;
};

}
}
