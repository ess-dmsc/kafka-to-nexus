#pragma once

#include "ConnectionStatusDatasets.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace pvCn {

class pvCn_Writer final : public WriterModule::Base {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FileWriter::FlatbufferMessage const &Message) override;

  pvCn_Writer() : WriterModule::Base(false, "NXlog") {}
  ~pvCn_Writer() override = default;

private:
  NeXusDataset::ConnectionStatusTime TimestampDataset;
  NeXusDataset::ConnectionStatus StatusDataset;
};

} // namespace pvCn
} // namespace WriterModule
