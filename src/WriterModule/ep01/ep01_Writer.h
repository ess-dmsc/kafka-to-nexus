#pragma once

#include "ConnectionStatusDatasets.h"
#include "WriterModuleBase.h"

namespace WriterModule::ep01 {

class ep01_Writer final : public WriterModule::Base {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void writeImpl(FileWriter::FlatbufferMessage const &Message,
                 bool is_buffered_message) override;

  ep01_Writer() : WriterModule::Base("ep01", false, "NXlog") {}
  ~ep01_Writer() override = default;

private:
  NeXusDataset::ConnectionStatusTime TimestampDataset;
  NeXusDataset::ConnectionStatus StatusDataset;
};

} // namespace WriterModule::ep01
