#include "Source.h"
#include "helper.h"
#include "logger.h"

namespace FileWriter {

Result Result::Ok() {
  Result ret;
  ret._res = 0;
  return ret;
}

Source::Source(std::string sourcename, HDFWriterModule::ptr hdf_writer_module)
    : _sourcename(std::move(sourcename)),
      _hdf_writer_module(std::move(hdf_writer_module)) {}

Source::Source(Source &&x) noexcept
    : _topic(std::move(x._topic)), _sourcename(std::move(x._sourcename)),
      _hdf_writer_module(std::move(x._hdf_writer_module)) {}

std::string const &Source::topic() const { return _topic; }

std::string const &Source::sourcename() const { return _sourcename; }

ProcessMessageResult Source::process_message(Msg const &msg) {
  if (!_hdf_writer_module) {
    throw "ASSERT FAIL: _hdf_writer_module";
  }
  auto &reader = FlatbufferReaderRegistry::find(msg);
  if (!reader->verify(msg)) {
    LOG(Sev::Error, "buffer not verified");
    return ProcessMessageResult::ERR();
  }
  auto ret = _hdf_writer_module->write(msg);
  _cnt_msg_written += 1;
  _processed_messages_count += 1;
  if (ret.is_ERR()) {
    return ProcessMessageResult::ERR();
  }
  if (ret.is_OK_WITH_TIMESTAMP()) {
    return ProcessMessageResult::OK(ret.timestamp());
  }
  return ProcessMessageResult::OK();
}

uint64_t Source::processed_messages_count() const {
  return _processed_messages_count;
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
  v.AddMember("source", Value().SetString(sourcename().data(), a), a);
  return jd;
}

} // namespace FileWriter
