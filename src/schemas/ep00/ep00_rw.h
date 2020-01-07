#pragma once

#include "../../HDFWriterModule.h"
#include "ConnectionStatusDatasets.h"
#include "FlatbufferReader.h"
#include <nlohmann/json.hpp>

namespace FileWriter {
namespace Schemas {
namespace ep00 {

class HDFWriterModule final : public FileWriter::HDFWriterModule {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  void parse_config(std::string const &ConfigurationStream) override;
  HDFWriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FlatbufferMessage const &Message) override;

  int32_t close() override;

  HDFWriterModule() = default;
  ~HDFWriterModule() override = default;
  size_t BufferSize = 0;
  size_t BufferPacketMax = 0;
  hsize_t ChunkBytes = 1024;

private:
  SharedLogger Logger = getLogger();

  NeXusDataset::ConnectionStatusTime TimestampDataset;
  NeXusDataset::ConnectionStatus StatusDataset;
};

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
