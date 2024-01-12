#pragma once

#include "ConnectionStatusDatasets.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ep01 {

class ep01_Writer final : public WriterModule::Base {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void writeImpl(FileWriter::FlatbufferMessage const &Message) override;

  ep01_Writer() : WriterModule::Base(false, "NXlog") {}
  ~ep01_Writer() override = default;

private:
  NeXusDataset::ConnectionStatusTime TimestampDataset;
  NeXusDataset::ConnectionStatus StatusDataset;
};

} // namespace ep01
} // namespace WriterModule
