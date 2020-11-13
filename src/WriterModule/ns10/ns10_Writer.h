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
///
/// \brief Writing module for the NICOS cache values.

#pragma once
#include "FlatbufferMessage.h"
#include "NeXusDataset/NeXusDataset.h"
#include "WriterModuleBase.h"

namespace WriterModule {
namespace ns10 {

class ns10_Writer : public WriterModule::Base {
public:
  ns10_Writer() : WriterModule::Base(false, "NXlog") {}
  ~ns10_Writer() override = default;

  void parse_config(std::string const &ConfigurationStream) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FileWriter::FlatbufferMessage const &Message) override;

protected:
  std::string Sourcename;
  NeXusDataset::DoubleValue Values;
  hdf5::Dimensions ChunkSize{1024};
  NeXusDataset::Time Timestamp;
  int CueInterval{1000};
  int CueCounter{0};
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};

} // namespace ns10
} // namespace WriterModule
