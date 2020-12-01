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
#include "Msg.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"
#include "WriterModuleConfig/Field.h"

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
  senv_Writer() : FileWriterBase(false, "NXlog") {}
  ~senv_Writer() override = default;

  void process_config() override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FlatbufferMessage const &Message) override;

protected:
  NeXusDataset::UInt16Value Value;
  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
  SharedLogger Logger = spdlog::get("filewriterlogger");
  WriterModuleConfig::Field<size_t> ChunkSize{this, "chunk_size", 8192};
};
} // namespace senv
} // namespace WriterModule
