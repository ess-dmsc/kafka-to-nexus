#include "Source.h"
#include "helper.h"
#include "logger.h"

namespace BrightnESS {
namespace FileWriter {

Result Result::Ok() {
  Result ret;
  ret._res = 0;
  return ret;
}

Source::Source(std::string topic, std::string source)
    : _topic(topic), _source(source) {}

Source::Source(Source &&x)
    : _topic(std::move(x._topic)), _source(std::move(x._source)),
      _broker(std::move(x._broker)), _hdf_path(std::move(x._hdf_path)),
      _schema_writer(std::move(x._schema_writer)) {
  using std::swap;
  swap(_config_file, x._config_file);
  swap(_config_stream, x._config_stream);
}

std::string const &Source::topic() const { return _topic; }

std::string const &Source::source() const { return _source; }

std::string const &Source::broker() const { return _broker; }

Source::~Source() {}

ProcessMessageResult Source::process_message(Msg msg) {
  if (!_schema_reader) {
    _schema_reader = FBSchemaReader::create(msg);
    if (_schema_writer) {
      LOG(0, "ERROR _schema_writer should not exist");
    }
    if (!_hdf_file) {
      throw "SHOULD NEVER HAPPEN AT LEAST CURRENTLY, HDF FILE SHOULD ALREADY "
            "BE OPEN";
    }
    _schema_writer = _schema_reader->create_writer();
  }
  if (!_schema_reader->verify(msg)) {
    LOG(5, "buffer not verified");
    return ProcessMessageResult::ERR();
  }
  if (teamid == _schema_reader->teamid(msg)) {
    if (_cnt_msg_written == 0) {
      _schema_writer->init(_hdf_file, _hdf_path, source(), msg, _config_file,
                           &_config_stream);
    }
    auto ret = _schema_writer->write(msg);
    _cnt_msg_written += 1;
    _processed_messages_count += 1;
    if (ret.ts < 0) {
      return ProcessMessageResult::ERR();
    }
    return ProcessMessageResult::OK(ret.ts);
  } else {
    // If it's not on the team, still fake success because otherwise
    // Streamer currently aborts.
    return ProcessMessageResult::OK(_schema_reader->ts(msg));
  }
  return ProcessMessageResult::ERR();
}

uint32_t Source::processed_messages_count() const {
  return _processed_messages_count;
}

void Source::hdf_init(HDFFile &hdf_file) { _hdf_file = &hdf_file; }

void Source::config_file(rapidjson::Value const *config_file) {
  this->_config_file = config_file;
}

void Source::config_stream(rapidjson::Document &&config_stream) {
  using std::swap;
  swap(this->_config_stream, config_stream);
}

std::string Source::to_str() const { return json_to_string(to_json()); }

rapidjson::Document
Source::to_json(rapidjson::MemoryPoolAllocator<> *_a) const {
  using namespace rapidjson;
  Document jd;
  if (_a)
    jd = Document(_a);
  auto &a = jd.GetAllocator();
  jd.SetObject();
  auto &v = jd;
  v.AddMember("__KLASS__", "Source", a);
  v.AddMember("topic", Value().SetString(topic().data(), a), a);
  v.AddMember("source", Value().SetString(source().data(), a), a);
  return jd;
}

} // namespace FileWriter
} // namespace BrightnESS
