/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Define classes required to implement the ADC file writing module.

#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../Msg.h"
#include "NeXusDataset.h"

namespace senv {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FBReaderBase = FileWriter::FlatbufferReader;

/// See parent class for documentation.
class SampleEnvironmentDataGuard : public FBReaderBase {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};

using FileWriterBase = FileWriter::HDFWriterModule;

std::vector<std::uint64_t> GenerateTimeStamps(std::uint64_t OriginTimeStamp,
                                              double TimeDelta,
                                              int NumberOfElements);

/// See parent class for documentation.
class FastSampleEnvironmentWriter : public FileWriterBase {
public:
  FastSampleEnvironmentWriter() = default;
  ~FastSampleEnvironmentWriter() override = default;

  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  WriteResult write(FlatbufferMessage const &Message) override;

  int32_t flush() override;

  int32_t close() override;

  void enable_cq(CollectiveQueue *cq, HDFIDStore *hdf_store,
                 int mpi_rank) override;

protected:
  NeXusDataset::RawValue Value;
  NeXusDataset::Time Timestamp;
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
};
} // namespace senv
