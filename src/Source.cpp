#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <fstream>
#include <thread>

#ifndef SOURCE_DO_PROCESS_MESSAGE
#define SOURCE_DO_PROCESS_MESSAGE 1
#endif

namespace FileWriter {

Result Result::Ok() {
  Result ret;
  ret._res = 0;
  return ret;
}

Source::Source(std::string sourcename, HDFWriterModule::ptr hdf_writer_module)
    : _sourcename(sourcename),
      _hdf_writer_module(std::move(hdf_writer_module)) {
  if (SOURCE_DO_PROCESS_MESSAGE == 0) {
    do_process_message = false;
  }
}

Source::Source(Source &&x) noexcept { swap(*this, x); }

void swap(Source &x, Source &y) {
  using std::swap;
  swap(x._topic, y._topic);
  swap(x._sourcename, y._sourcename);
  swap(x._hdf_writer_module, y._hdf_writer_module);
  swap(x._processed_messages_count, y._processed_messages_count);
  swap(x._cnt_msg_written, y._cnt_msg_written);
  swap(x.do_process_message, y.do_process_message);
  swap(x.is_parallel, y.is_parallel);
}

std::string const &Source::topic() const { return _topic; }

std::string const &Source::sourcename() const { return _sourcename; }

ProcessMessageResult Source::process_message(Msg &msg) {
  auto &reader = FlatbufferReaderRegistry::find(msg);
  if (!reader->verify(msg)) {
    LOG(Sev::Error, "buffer not verified");
    return ProcessMessageResult::ERR();
  }
  if (!do_process_message) {
    return ProcessMessageResult::OK();
  }
  if (!is_parallel) {
    if (!_hdf_writer_module) {
      LOG(Sev::Debug, "!_hdf_writer_module for {}", _sourcename);
      return ProcessMessageResult::ERR();
    }
    auto ret = _hdf_writer_module->write(msg);
    _cnt_msg_written += 1;
    _processed_messages_count += 1;
    if (ret.is_ERR()) {
      return ProcessMessageResult::ERR();
    }
    if (HDFFileForSWMR) {
      HDFFileForSWMR->SWMRFlush();
    }
    if (ret.is_OK_WITH_TIMESTAMP()) {
      return ProcessMessageResult::OK(ret.timestamp());
    }
    return ProcessMessageResult::OK();
  }
  return ProcessMessageResult::ERR();
}

uint64_t Source::processed_messages_count() const {
  return _processed_messages_count;
}

void Source::close_writer_module() { _hdf_writer_module.reset(); }

std::string Source::to_str() const { return to_json().dump(); }

nlohmann::json Source::to_json() const {
  auto JSON = nlohmann::json::object();
  JSON["__KLASS__"] = "Source";
  JSON["topic"] = topic();
  JSON["source"] = sourcename();
  return JSON;
}

} // namespace FileWriter
