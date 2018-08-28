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

Source::Source(std::string const &Name, std::string const &ID,
               HDFWriterModule::ptr Writer)
    : SourceName(Name), SchemaID(ID), WriterModule(std::move(Writer)) {
  if (SOURCE_DO_PROCESS_MESSAGE == 0) {
    do_process_message = false;
  }
}

Source::~Source() { close_writer_module(); }

Source::Source(Source &&x) noexcept { swap(*this, x); }

void swap(Source &x, Source &y) {
  std::swap(x._topic, y._topic);
  std::swap(x.SourceName, y.SourceName);
  std::swap(x.SchemaID, y.SchemaID);
  std::swap(x.WriterModule, y.WriterModule);
  std::swap(x._processed_messages_count, y._processed_messages_count);
  std::swap(x._cnt_msg_written, y._cnt_msg_written);
  std::swap(x.do_process_message, y.do_process_message);
  std::swap(x.is_parallel, y.is_parallel);
}

std::string const &Source::topic() const { return _topic; }

std::string const &Source::sourcename() const { return SourceName; }

ProcessMessageResult Source::process_message(FlatbufferMessage const &Message) {
  if (std::string(Message.data() + 4, Message.data() + 8) != SchemaID) {
    LOG(Sev::Debug, "SchemaID: {} not accepted by source_name: {}", SchemaID,
        SourceName);
    return ProcessMessageResult::ERR;
  }

  if (!do_process_message) {
    return ProcessMessageResult::OK;
  }
  if (!is_parallel) {
    if (!WriterModule) {
      LOG(Sev::Debug, "!_hdf_writer_module for {}", SourceName);
      return ProcessMessageResult::ERR;
    }
    auto ret = WriterModule->write(Message);
    _cnt_msg_written += 1;
    _processed_messages_count += 1;
    if (ret.is_ERR()) {
      return ProcessMessageResult::ERR;
    }
    if (HDFFileForSWMR) {
      HDFFileForSWMR->SWMRFlush();
    }
    return ProcessMessageResult::OK;
  }
  return ProcessMessageResult::ERR;
}

uint64_t Source::processed_messages_count() const {
  return _processed_messages_count;
}

void Source::close_writer_module() {
  if (WriterModule) {
    LOG(Sev::Debug, "Closing writer module for {}", SourceName);
    WriterModule->flush();
    WriterModule->close();
    WriterModule.reset();
    LOG(Sev::Debug, "Writer module closed for {}", SourceName);
  } else {
    LOG(Sev::Debug, "No writer module to close for {}", SourceName);
  }
}

std::string Source::to_str() const { return to_json().dump(); }

nlohmann::json Source::to_json() const {
  auto JSON = nlohmann::json::object();
  JSON["__KLASS__"] = "Source";
  JSON["topic"] = topic();
  JSON["source"] = sourcename();
  return JSON;
}

} // namespace FileWriter
