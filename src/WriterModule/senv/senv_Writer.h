// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Define classes required to implement the ADC file writing module.

#pragma once

#include "FlatbufferMessage.h"
#include "HDFFile.h"
#include "JsonConfig/Field.h"
#include "Msg.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace senv {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FileWriterBase = WriterModule::Base;

std::vector<std::uint64_t> GenerateTimeStamps(std::uint64_t OriginTimeStamp,
                                              double TimeDelta,
                                              int NumberOfElements);

/// See parent class for documentation.
class senv_Writer : public FileWriterBase {
public:
  senv_Writer()
      : FileWriterBase(false, "NXlog", {"epics_con_status", "alarm_status"}) {}
  ~senv_Writer() override = default;

  void config_post_processing() override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup) const override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FlatbufferMessage const &Message) override;

  enum class Type {
    int8,
    uint8,
    int16,
    uint16,
    int32,
    uint32,
    int64,
    uint64,
  };

protected:
  void initValueDataset(hdf5::node::Group const &Parent) const;
  Type ElementType{Type::int64};
  NeXusDataset::ExtensibleDatasetBase Value;
  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
  JsonConfig::Field<size_t> ChunkSize{this, "chunk_size", 4096};
  JsonConfig::Field<std::string> DataType{this, {"type", "dtype"}, "int64"};
  bool HasCheckedMessageType{false};
};
} // namespace senv
} // namespace WriterModule
