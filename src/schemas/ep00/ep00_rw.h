#pragma once

#include "../../HDFWriterModule.h"
#include "../../h5.h"
#include "FlatbufferReader.h"
#include <nlohmann/json.hpp>

namespace FileWriter {
namespace Schemas {
namespace ep00 {

enum class ChannelConnectionState : uint8_t {
  UNKNOWN,
  NEVER_CONNECTED,
  CONNECTED,
  DISCONNECTED,
  DESTROYED,
};

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
  /// Name of the source that we write.
  std::string SourceName;
  /// Datatype as given in the filewriter json command.
  std::string TypeName;
  SharedLogger Logger = spdlog::get("filewriterlogger");
  static bool findType(const nlohmann::basic_json<> Attribute,
                       std::string &DType);
  std::unique_ptr<h5::Chunked1DString> Value;
  std::unique_ptr<h5::h5d_chunked_1d<uint64_t>> Timestamp;
  size_t buffer_size = 10;
  size_t buffer_packet_max = 15;
};

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
