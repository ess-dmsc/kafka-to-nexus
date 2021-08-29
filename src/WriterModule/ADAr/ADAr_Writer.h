// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2021 European Spallation Source ERIC */

/// \file
/// \brief Define classes required to implement the ADC file writing module.

#pragma once

#include "FlatbufferMessage.h"
#include "HDFFile.h"
#include "Msg.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ADAr {
/// See parent class for documentation.
class ADAr_Writer : public WriterModule::Base {
public:
  ADAr_Writer() : WriterModule::Base(false, "NXlog") {}
  ~ADAr_Writer() override = default;

  void config_post_processing() override;

  InitResult init(hdf5::node::Group &HDFGroup, MetaData::TrackerPtr Tracker) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FileWriter::FlatbufferMessage const &Message) override;

protected:
  void initValueDataset(hdf5::node::Group const &Parent);
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
    c_string,
  } ElementType{Type::float64};
  std::unique_ptr<NeXusDataset::MultiDimDatasetBase> Values;
  NeXusDataset::Time Timestamp;
  JsonConfig::Field<int> CueInterval{this, "cue_interval", 1000};
  JsonConfig::Field<std::string> DataType{this, {"type", "dtype"}, "float64"};
  JsonConfig::Field<hdf5::Dimensions> ArrayShape{this, "array_size", {1, 1}};
  JsonConfig::Field<hdf5::Dimensions> ChunkSize{this, "chunk_size", {1 << 20}};
  int CueCounter{0};
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace ADAr
} // namespace WriterModule
