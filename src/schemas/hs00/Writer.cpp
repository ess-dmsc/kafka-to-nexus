#include "Writer.h"
#include "Exceptions.h"

namespace FileWriter {
namespace Schemas {
namespace hs00 {

void Writer::parse_config(std::string const &ConfigurationStream,
                          std::string const &ConfigurationModule) {
  throw unimplemented();
}

FileWriter::HDFWriterModule::InitResult
Writer::init_hdf(hdf5::node::Group &HDFGroup,
                 std::string const &HDFAttributes) {
  throw unimplemented();
  return FileWriter::HDFWriterModule::InitResult::OK();
}

FileWriter::HDFWriterModule::InitResult
Writer::reopen(hdf5::node::Group &HDFGroup) {
  throw unimplemented();
  return FileWriter::HDFWriterModule::InitResult::OK();
}

FileWriter::HDFWriterModule::WriteResult
Writer::write(FlatbufferMessage const &Message) {
  throw unimplemented();
  return FileWriter::HDFWriterModule::WriteResult::OK();
}

int32_t Writer::flush() {
  throw unimplemented();
  return 0;
}

int32_t Writer::close() {
  throw unimplemented();
  return 0;
}

void Writer::enable_cq(CollectiveQueue *, HDFIDStore *, int) {}

FileWriter::HDFWriterModule::ptr Writer::create() {
  return FileWriter::HDFWriterModule::ptr(new Writer);
}
}
}
}
