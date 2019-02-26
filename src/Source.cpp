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
    Logger->trace("SchemaID: {} not accepted by source_name: {}", SchemaID,
                  SourceName);
    return ProcessMessageResult::ERR;
  }

  if (!is_parallel) {
    if (!WriterModule) {
      Logger->trace("!_hdf_writer_module for {}", SourceName);
      return ProcessMessageResult::ERR;
    }
    auto ret = WriterModule->write(Message);
    _cnt_msg_written += 1;
    _processed_messages_count += 1;
    if (ret.is_ERR()) {
      Logger->trace("Failure while writing message: {}", ret.to_str());
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
  // TODO: should log here using Source::Logger field but it's been std::moved
  // in CommandHandler::addStreamSourceToWriterModule() and would be NULL here
  if (WriterModule) {
    spdlog::get("filewriterlogger")
        ->trace("Closing writer module for {}", SourceName);
    WriterModule->flush();
    WriterModule->close();
    WriterModule.reset();
    spdlog::get("filewriterlogger")
        ->trace("Writer module closed for {}", SourceName);
  } else {
    spdlog::get("filewriterlogger")
        ->trace("No writer module to close for {}", SourceName);
  }
}

void Source::setTopic(std::string const &Name) { TopicName = Name; }

} // namespace FileWriter
