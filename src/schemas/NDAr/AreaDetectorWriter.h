/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
/// \brief Define classes required to implement the ADC file writing module.

#pragma once

#include "../../FlatbufferReader.h"
#include "../../HDFFile.h"
#include "../../HDFWriterModule.h"
#include "../../Msg.h"
#include "NeXusDataset.h"
#include "schemas/NDAr_NDArray_schema_generated.h"

namespace NDAr {
using FlatbufferMessage = FileWriter::FlatbufferMessage;
using FBReaderBase = FileWriter::FlatbufferReader;

/// See parent class for documentation.
class AreaDetectorDataGuard : public FBReaderBase {
public:
  bool verify(FlatbufferMessage const &Message) const override;
  std::string source_name(FlatbufferMessage const &Message) const override;
  uint64_t timestamp(FlatbufferMessage const &Message) const override;
};

using FileWriterBase = FileWriter::HDFWriterModule;

std::uint64_t epicsTimeToNsec(std::uint64_t sec, std::uint64_t nsec);

/// See parent class for documentation.
class AreaDetectorWriter : public FileWriterBase {
public:
  AreaDetectorWriter() = default;
  ~AreaDetectorWriter() = default;

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
  void initValueDataset(hdf5::node::Group &Parent);
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
  hdf5::Dimensions ArrayShape{1, 1};
  hdf5::Dimensions ChunkSize{64};
  std::unique_ptr<NeXusDataset::MultiDimDatasetBase> Values;
  NeXusDataset::Time Timestamp;
  int CueInterval{1000};
  int CueCounter{0};
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;
};
} // namespace NDAr
