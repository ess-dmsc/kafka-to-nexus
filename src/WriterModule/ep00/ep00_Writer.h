#pragma once

#include "ConnectionStatusDatasets.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ep00 {

class ep00_Writer final : public WriterModule::Base {
public:
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  void parse_config(std::string const &ConfigurationStream) override;
  InitResult reopen(hdf5::node::Group &HDFGroup) override;
  void write(FileWriter::FlatbufferMessage const &Message) override;

  ep00_Writer() = default;
  ~ep00_Writer() override = default;

private:
  SharedLogger Logger = getLogger();

  NeXusDataset::ConnectionStatusTime TimestampDataset;
  NeXusDataset::ConnectionStatus StatusDataset;
};

} // namespace ep00
} // namespace WriterModule
