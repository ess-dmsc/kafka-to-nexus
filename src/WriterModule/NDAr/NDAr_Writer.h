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

namespace WriterModule {
namespace NDAr {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FileWriterBase = WriterModule::Base;

using std::string_literals::operator""s;

/// See parent class for documentation.
class NDAr_Writer : public FileWriterBase {
public:
  NDAr_Writer() : WriterModule::Base(false, "NXlog") {}
  ~NDAr_Writer() override = default;

  void process_config() override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FlatbufferMessage const &Message) override;

  static std::uint64_t epicsTimeToNsec(std::uint64_t sec, std::uint64_t nsec);

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

  WriterModuleConfig::Field<std::string> DataType{
      this, std::initializer_list<std::string>({"type"s, "dtype"s}), "float64"s};
  WriterModuleConfig::Field<int> CueInterval{this, "cue_interval", 1000};
  WriterModuleConfig::Field<hdf5::Dimensions> ChunkSize{
      this, "chunk_size", {1024}};
  WriterModuleConfig::Field<hdf5::Dimensions> ArrayShape{
      this, "array_size", {1, 1}};

  std::unique_ptr<NeXusDataset::MultiDimDatasetBase> Values;
  NeXusDataset::Time Timestamp;
  int CueCounter{0};
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace NDAr
} // namespace WriterModule
