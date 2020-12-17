#pragma once

#include "ConnectionStatusDatasets.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ep00 {

class ep00_Writer final : public WriterModule::Base {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FileWriter::FlatbufferMessage const &Message) override;

  ep00_Writer() : WriterModule::Base(false, "NXlog") {}
  ~ep00_Writer() override = default;

private:
  SharedLogger Logger = getLogger();

  NeXusDataset::ConnectionStatusTime TimestampDataset;
  NeXusDataset::ConnectionStatus StatusDataset;
  JsonConfig::Field<size_t> ChunkSize{this, "chunk_size", 1024};
};

} // namespace ep00
} // namespace WriterModule
