#include "Writer.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

void Writer::parse_config(std::string const &ConfigurationStream,
                          std::string const &ConfigurationModule) {
  TheWriterUntyped = WriterUntyped::createFromJson(
      WriterUntyped::json::parse(ConfigurationStream));
}

FileWriter::HDFWriterModule::InitResult
Writer::init_hdf(hdf5::node::Group &HDFGroup,
                 std::string const &HDFAttributes) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped->createHDFStructure(HDFGroup, ChunkBytes);
  return FileWriter::HDFWriterModule::InitResult::OK();
}

FileWriter::HDFWriterModule::InitResult
Writer::reopen(hdf5::node::Group &HDFGroup) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  TheWriterUntyped = WriterUntyped::createFromHDF(HDFGroup);
  if (!TheWriterUntyped) {
    return FileWriter::HDFWriterModule::InitResult::ERROR_IO();
  }
  return FileWriter::HDFWriterModule::InitResult::OK();
}

FileWriter::HDFWriterModule::WriteResult
Writer::write(FlatbufferMessage const &Message) {
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized. Make sure "
                             "that you call parse_config() before.");
  }
  auto Result = TheWriterUntyped->write(Message, DoFlushEachWrite);
  if (!Result.is_OK()) {
    LOG(Sev::Error, "hs00 write error: {}", Result.to_str());
  }
  return Result;
}

int32_t Writer::flush() {
  LOG(Sev::Debug, "flush");
  return 0;
}

int32_t Writer::close() {
  LOG(Sev::Debug, "close");
  if (!TheWriterUntyped) {
    throw std::runtime_error("TheWriterUntyped is not initialized.");
  }
  return TheWriterUntyped->close();
}

void Writer::enable_cq(CollectiveQueue *, HDFIDStore *, int) {}

FileWriter::HDFWriterModule::ptr Writer::create() {
  return FileWriter::HDFWriterModule::ptr(new Writer);
}

HDFWriterModuleRegistry::Registrar<Writer> RegisterWriter("hs00");
}
}
}
