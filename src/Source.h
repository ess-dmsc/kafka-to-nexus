#pragma once
#include <string>
#include "TimeDifferenceFromMessage.h"
#include "HDFFile.h"
#include "json.h"

class Test___FileWriterTask___Create01;

namespace BrightnESS {
namespace FileWriter {

class SourceFactory_by_FileWriterTask;

class Result {
public:
static Result Ok();
bool is_OK();
bool is_ERR();
private:
int _res = 0;
};

/// Represents a sourcename on a topic.
/// The sourcename can be empty.
/// This is meant for highest efficiency on topics which are exclusively used for only one sourcename.
class Source {
public:
Source(Source &&);
std::string const & topic() const;
std::string const & source() const;
Result process_message(char * msg_data, int msg_size);
std::string to_str() const;
rapidjson::Document to_json(rapidjson::MemoryPoolAllocator<> * a = nullptr) const;
private:
Source(std::string topic, std::string source);
/// Used by FileWriterTask during setup.
void hdf_init(HDFFile & hdf_file);

std::string _topic;
std::string _source;
std::unique_ptr<FBSchemaReader> _schema_reader;
std::unique_ptr<FBSchemaWriter> _schema_writer;

HDFFile * _hdf_file = nullptr;

friend class CommandHandler;
friend class FileWriterTask;
friend class SourceFactory_by_FileWriterTask;
friend class ::Test___FileWriterTask___Create01;
};

}
}
