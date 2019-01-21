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

std::string const &Source::topic() const { return Topic_; }

std::string const &Source::sourcename() const { return SourceName; }

ProcessMessageResult Source::process_message(FlatbufferMessage const &Message) {
  if (std::string(Message.data() + 4, Message.data() + 8) != SchemaID) {
    LOG(spdlog::level::trace, "SchemaID: {} not accepted by source_name: {}", SchemaID,
        SourceName);
    return ProcessMessageResult::ERR;
  }

  if (!is_parallel) {
    if (!WriterModule) {
      LOG(spdlog::level::trace, "!_hdf_writer_module for {}", SourceName);
      return ProcessMessageResult::ERR;
    }
    auto ret = WriterModule->write(Message);
    _cnt_msg_written += 1;
    _processed_messages_count += 1;
    if (ret.is_ERR()) {
        LOG(spdlog::level::trace, "Failure while writing message: {}", ret.to_str());
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
    LOG(spdlog::level::trace, "Closing writer module for {}", SourceName);
    WriterModule->flush();
    WriterModule->close();
    WriterModule.reset();
    LOG(spdlog::level::trace, "Writer module closed for {}", SourceName);
  } else {
    LOG(spdlog::level::trace, "No writer module to close for {}", SourceName);
  }
}

void Source::setTopic(std::string const &Name) { Topic_ = Name; }

} // namespace FileWriter
