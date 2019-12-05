// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "../../HDFWriterModule.h"
#include <NeXusDataset.h>
#include <array>
#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>
#include <vector>

namespace FileWriter {
namespace Schemas {
namespace f142 {

using ForwarderDebugDataset = NeXusDataset::ExtensibleDataset<std::uint64_t>;

class f142Writer : public FileWriter::HDFWriterModule {
public:
  /// Implements HDFWriterModule interface.
  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;
  /// Implements HDFWriterModule interface.
  void parse_config(std::string const &ConfigurationStream) override;
  /// Implements HDFWriterModule interface.
  f142Writer::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Write an incoming message which should contain a flatbuffer.
  void write(FlatbufferMessage const &Message) override;

  int32_t close() override { return 0; };

  f142Writer();
  ~f142Writer() override = default;

  enum class Type {
    int8,
    uint8,
    int16,
    uint16,
    int32,
    uint32,
    int64,
    uint64,
    float32,
    float64,
  };

protected:
  SharedLogger Logger = spdlog::get("filewriterlogger");
  std::string findDataType(nlohmann::basic_json<> const &Attribute);

  Type ElementType{Type::float64};

  NeXusDataset::MultiDimDatasetBase Values;

  /// Timestamps of the f142 updates.
  NeXusDataset::Time Timestamp;

  /// Index into DatasetTimestamp.
  NeXusDataset::CueTimestampZero CueTimestampZero;

  /// Index into the f142 values.
  NeXusDataset::CueIndex CueIndex;

  // set by default to a large value:
  uint64_t ValueIndexInterval = std::numeric_limits<uint64_t>::max();
  size_t ArraySize{1};
  size_t ChunkSize{64 * 1024};
  std::string ValueUnits;
};

} // namespace f142
} // namespace Schemas
} // namespace FileWriter
