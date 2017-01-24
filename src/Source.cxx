#include "Source.h"
#include "logger.h"

namespace BrightnESS {
namespace FileWriter {

Result Result::Ok() {
	Result ret;
	ret._res = 0;
	return ret;
}

Source::Source(std::string topic, std::string source)
	:	_topic(topic),
		_source(source) {
}

Source::Source(Source && x) :
	_topic(std::move(x._topic)),
	_source(std::move(x._source)),
	_schema_writer(std::move(x._schema_writer))
{	
}

std::string const & Source::topic() const {
	return _topic;
}

std::string const & Source::source() const {
	return _source;
}

Result Source::process_message(char * msg_data, int msg_size) {
	if (!_schema_reader) {
		_schema_reader = FBSchemaReader::create(msg_data, msg_size);
	}
	if (!_schema_writer) {
		_schema_writer = _schema_reader->create_writer();
		if (!_hdf_file) {
			throw "SHOULD NEVER HAPPEN";
		}
		_schema_writer->init(*_hdf_file, msg_data);
	}
	_schema_writer->write(msg_data);
	_processed_messages_count += 1;
	return Result::Ok();
}

uint32_t Source::processed_messages_count() const {
	return _processed_messages_count;
}

void Source::hdf_init(HDFFile & hdf_file) {
	_hdf_file = &hdf_file;
}

std::string Source::to_str() const {
	return json_to_string(to_json());
}

rapidjson::Document Source::to_json(rapidjson::MemoryPoolAllocator<> * _a) const {
	using namespace rapidjson;
	Document jd;
	if (_a) jd = Document(_a);
	auto & a = jd.GetAllocator();
	jd.SetObject();
	auto & v = jd;
	v.AddMember("__KLASS__", "Source", a);
	v.AddMember("topic", Value().SetString(topic().data(), a), a);
	v.AddMember("source", Value().SetString(source().data(), a), a);
	return jd;
}


}
}
