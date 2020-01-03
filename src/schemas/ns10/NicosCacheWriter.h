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
#include "../../HDFWriterModule.h"
#include "NeXusDataset.h"

namespace FileWriter {
namespace Schemas {
namespace ns10 {

class CacheWriter : public FileWriter::HDFWriterModule {
public:
  CacheWriter() = default;
  ~CacheWriter() override = default;

  void parse_config(std::string const &ConfigurationStream) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

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
} // namespace Schemas
} // namespace FileWriter
