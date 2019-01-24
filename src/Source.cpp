#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <fstream>
#include <thread>

namespace FileWriter {

Source::Source(std::string Name, std::string ID, HDFWriterModule::ptr Writer)
    : SourceName(std::move(Name)), SchemaID(std::move(ID)),
      WriterModule(std::move(Writer)) {}

Source::~Source() { close_writer_module(); }

std::string const &Source::topic() const { return TopicName; }

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
    if (HDFFileForSWMR != nullptr) {
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

void Source::setTopic(std::string const &Name) { TopicName = Name; }

} // namespace FileWriter
