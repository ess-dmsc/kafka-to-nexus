#pragma once

#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "FlatbufferReader.h"
#include <nlohmann/json.hpp>

namespace FileWriter {
namespace Schemas {
namespace ep00 {

class HDFWriterModule final : public FileWriter::HDFWriterModule {
public:
  /// Implements HDFWriterModule interface.
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  /// Implements HDFWriterModule interface.
  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;
  /// Implements HDFWriterModule interface.
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Actual initialziation code, mostly shared among CREATE and OPEN phases.
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const *HDFAttributes,
                      CreateWriterTypedBaseMethod CreateMethod);

  /// Write an incoming message which should contain a flatbuffer.
  void write(FlatbufferMessage const &Message) override;

  /// Flush underlying buffers.
  int32_t flush() override;
  int32_t close() override;

  HDFWriterModule();
  ~HDFWriterModule() override = default;
  size_t BufferSize = 0;
  size_t BufferPacketMax = 0;
  hsize_t ChunkBytes = 1024;

private:
  SharedLogger Logger = getLogger();
  std::unique_ptr<h5::Chunked1DString> AlarmStatus;
  std::unique_ptr<h5::h5d_chunked_1d<uint64_t>> AlarmTimestamp;
};

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
