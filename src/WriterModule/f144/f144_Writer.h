// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "FlatbufferMessage.h"
#include "JsonConfig/Field.h"
#include "MetaData/Value.h"
#include "WriterModuleBase.h"
#include <NeXusDataset/EpicsAlarmDatasets.h>
#include <NeXusDataset/NeXusDataset.h>
#include <array>
#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <vector>

namespace WriterModule::f144 {
using FlatbufferMessage = FileWriter::FlatbufferMessage;

class f144_Writer : public WriterModule::Base {
public:
  /// Implements writer module interface.
  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;
  /// Implements writer module interface.
  void config_post_processing() override;
  /// Implements writer module interface.

  void register_meta_data(hdf5::node::Group const &HDFGroup,
                          MetaData::TrackerPtr const &Tracker) override;

  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Write an incoming message which should contain a flatbuffer.
  void writeImpl(FlatbufferMessage const &Message) override;

  f144_Writer()
      : WriterModule::Base("f144", false, "NXlog",
                           {"epics_con_info", "alarm_info"}) {}
  ~f144_Writer() override = default;

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
  Type ElementType{Type::float64};

  NeXusDataset::ExtensibleDatasetBase Values;

  /// Timestamps of the f144 updates.
  NeXusDataset::Time Timestamp;

  /// Index into DatasetTimestamp.
  NeXusDataset::CueTimestampZero CueTimestampZero;

  /// Index into the f144 values.
  NeXusDataset::CueIndex CueIndex;

  JsonConfig::Field<uint32_t> ValueIndexInterval{
      this, "cue_interval", std::numeric_limits<uint32_t>::max()};
  JsonConfig::Field<size_t> ArraySize{this, "array_size", 1};
  JsonConfig::Field<size_t> ChunkSize{this, "chunk_size", 1024};
  JsonConfig::Field<std::string> DataType{this, {"type", "dtype"}, "double"};
  JsonConfig::Field<std::string> Unit{this, {"value_units", "unit"}, ""};
  JsonConfig::Field<bool> MetaData{this, "meta_data", true};

  MetaData::Value<double> MetaDataMin{"", "minimum_value"};
  MetaData::Value<double> MetaDataMax{"", "maximum_value"};
  MetaData::Value<double> MetaDataMean{"", "average_value"};
  double Min{0};
  double Max{0};
  double Sum{0};
  uint64_t LastIndexAtWrite{0};
  uint64_t NrOfWrites{0};
  uint64_t TotalNrOfElementsWritten{0};
  bool HasCheckedMessageType{false};
};

} // namespace WriterModule::f144
