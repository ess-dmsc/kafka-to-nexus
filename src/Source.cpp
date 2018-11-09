#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <fstream>
#include <thread>

namespace FileWriter {

Source::Source(std::string const &Name, std::string const &ID,
               HDFWriterModule::ptr Writer)
    : SourceName(Name), SchemaID(ID), WriterModule(std::move(Writer)) {}

Source::~Source() { close_writer_module(); }

Source::Source(Source &&x) noexcept { swap(*this, x); }

void swap(Source &x, Source &y) {
  std::swap(x.Topic_, y.Topic_);
  std::swap(x.SourceName, y.SourceName);
  std::swap(x.SchemaID, y.SchemaID);
  std::swap(x.WriterModule, y.WriterModule);
  std::swap(x._processed_messages_count, y._processed_messages_count);
  std::swap(x._cnt_msg_written, y._cnt_msg_written);
  std::swap(x.is_parallel, y.is_parallel);
}

std::string const &Source::topic() const { return Topic_; }

std::string const &Source::sourcename() const { return SourceName; }

ProcessMessageResult Source::process_message(FlatbufferMessage const &Message) {
  if (std::string(Message.data() + 4, Message.data() + 8) != SchemaID) {
    LOG(Sev::Debug, "SchemaID: {} not accepted by source_name: {}", SchemaID,
        SourceName);
    return ProcessMessageResult::ERR;
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
      if (log_level >= static_cast<int>(Sev::Debug)) {
        LOG(Sev::Debug, "Failure while writing message: {}", ret.to_str());
      }
      return ProcessMessageResult::ERR;
    }
    if (HDFFileForSWMR) {
      HDFFileForSWMR->SWMRFlush();
    }
    return ProcessMessageResult::OK;
  }
  return ProcessMessageResult::ERR;
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
} // namespace FileWriter
