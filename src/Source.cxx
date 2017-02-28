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

std::string const & Source::broker() const {
	return _broker;
}

Source::~Source() {
}

ProcessMessageResult Source::process_message(Msg msg) {
	if (!_schema_reader) {
		_schema_reader = FBSchemaReader::create(msg);
		if (_schema_writer) {
			LOG(7, "ERROR _schema_writer should not exist");
		}
		if (!_hdf_file) {
			throw "SHOULD NEVER HAPPEN AT LEAST CURRENTLY, HDF FILE SHOULD ALREADY BE OPEN";
		}
		_schema_writer = _schema_reader->create_writer();
	}
	if (teamid == _schema_reader->teamid(msg)) {
		if (_cnt_msg_written == 0) {
			_schema_writer->init(_hdf_file, source(), msg);
		}
		auto ret = _schema_writer->write(msg);
		_cnt_msg_written += 1;
		_processed_messages_count += 1;
		return ProcessMessageResult::OK(ret.ts);
	}
	else {
		// If it's not on the team, still fake success because otherwise
		// Streamer currently aborts.
		return ProcessMessageResult::OK(_schema_reader->ts(msg));
	}
	return ProcessMessageResult::ERR();
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
