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
#include <al00_alarm_generated.h>
#include <array>
#include <chrono>
#include <f142_logdata_generated.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <vector>

namespace WriterModule {
namespace f142 {
using FlatbufferMessage = FileWriter::FlatbufferMessage;

class f142_Writer : public WriterModule::Base {
public:
  /// Implements writer module interface.
  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;
  /// Implements writer module interface.
  void config_post_processing() override;
  /// Implements writer module interface.

  void register_meta_data(hdf5::node::Group const &HDFGroup,
                          MetaData::TrackerPtr const &Tracker) override;

  WriterModule::InitResult reopen(hdf5::node::Group &HDFGroup) override;

  /// Write an incoming message which should contain a flatbuffer.
  void write(FlatbufferMessage const &Message) override;

  f142_Writer()
      : WriterModule::Base(false, "NXlog", {"epics_con_status"}),
        MetaDataMin("", "minimum_value"), MetaDataMax("", "maximum_value"),
        MetaDataMean("", "average_value") {}
  ~f142_Writer() override = default;

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

  NeXusDataset::MultiDimDatasetBase Values;

  /// Timestamps of the f142 updates.
  NeXusDataset::Time Timestamp;

  /// Index into DatasetTimestamp.
  NeXusDataset::CueTimestampZero CueTimestampZero;

  /// Index into the f142 values.
  NeXusDataset::CueIndex CueIndex;

  /// Timestamps of changes in EPICS alarm status
  NeXusDataset::AlarmTime AlarmTime;

  /// Changes in EPICS alarm status
  NeXusDataset::AlarmStatus AlarmStatus;

  /// Severity corresponding to EPICS alarm status
  NeXusDataset::AlarmSeverity AlarmSeverity;

  JsonConfig::Field<uint32_t> ValueIndexInterval{
      this, "cue_interval", std::numeric_limits<uint32_t>::max()};
  JsonConfig::Field<size_t> ArraySize{this, "array_size", 1};
  JsonConfig::Field<size_t> ChunkSize{this, "chunk_size", 1024};
  JsonConfig::Field<std::string> DataType{this, {"type", "dtype"}, "double"};
  JsonConfig::Field<std::string> Unit{this, {"value_units", "unit"}, ""};
  JsonConfig::Field<bool> MetaData{this, "meta_data", true};

  MetaData::Value<double> MetaDataMin;
  MetaData::Value<double> MetaDataMax;
  MetaData::Value<double> MetaDataMean;
  double Min{0}, Max{0}, Sum{0};
  uint64_t LastIndexAtWrite{0};
  uint64_t NrOfWrites{0};
  uint64_t TotalNrOfElementsWritten{0};
  bool HasCheckedMessageType{false};
};

inline std::unordered_map<std::int16_t, std::int16_t>
    F142SeverityToAl00Severity{
        {static_cast<std::int16_t>(AlarmSeverity::MINOR),
         static_cast<std::int16_t>(Severity::MINOR)},
        {static_cast<std::int16_t>(AlarmSeverity::MAJOR),
         static_cast<std::int16_t>(Severity::MAJOR)},
        {static_cast<std::int16_t>(AlarmSeverity::NO_ALARM),
         static_cast<std::int16_t>(Severity::OK)},
        {static_cast<std::int16_t>(AlarmSeverity::INVALID),
         static_cast<std::int16_t>(Severity::INVALID)},
        {static_cast<std::int16_t>(AlarmSeverity::NO_CHANGE),
         static_cast<std::int16_t>(Severity::INVALID)}};

} // namespace f142
} // namespace WriterModule
