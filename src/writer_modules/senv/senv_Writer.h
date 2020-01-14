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
#include "HDFWriterModule.h"
#include "Msg.h"
#include "NeXusDataset.h"

namespace Module {
namespace senv {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FileWriterBase = FileWriter::HDFWriterModule;

std::vector<std::uint64_t> GenerateTimeStamps(std::uint64_t OriginTimeStamp,
                                              double TimeDelta,
                                              int NumberOfElements);

/// See parent class for documentation.
class senv_Writer : public FileWriterBase {
public:
  senv_Writer() = default;
  ~senv_Writer() override = default;

  void parse_config(std::string const &) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FlatbufferMessage const &Message) override;

protected:
  NeXusDataset::UInt16Value Value;
  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace senv
} // namespace Module
