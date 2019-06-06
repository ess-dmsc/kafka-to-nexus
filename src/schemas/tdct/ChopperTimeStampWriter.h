/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Define classes required for chopper time stamp writing.

#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "NeXusDataset.h"

namespace tdct {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FBReaderBase = FileWriter::FlatbufferReader;

/// See parent class for documentation.
class ChopperTimeStampGuard : public FBReaderBase {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};

using FileWriterBase = FileWriter::HDFWriterModule;

/// See parent class for documentation.
class ChopperTimeStampWriter : public FileWriterBase {
public:
  ChopperTimeStampWriter() = default;
  ~ChopperTimeStampWriter() override = default;

  void parse_config(std::string const &, std::string const &) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FlatbufferMessage const &Message) override;

  int32_t flush() override;

  int32_t close() override;

protected:
  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace tdct
