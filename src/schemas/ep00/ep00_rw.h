#pragma once

#include "../../HDFWriterModule.h"
#include "FlatbufferReader.h"

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

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
