/** Copyright (C) 2018 European Spallation Source ERIC */

/// \file
///
/// \brief This file acts as a template for creating file writing modules.
///
/// All of the classes required are explained here. The
/// only thing missing from this header file is the registration of the file
/// writing module which must reside in an implementation file. See the
/// accompanying TestWriter.cpp for instructions on how this should be done.
///
/// A file writing module comprises two classes. The first class is the
/// flatbuffer reader which must implement the FileWriter::FlatbufferReader
/// interface. The virtual functions to be overridden are called to verify the
/// flatbuffer contents and extract a source name and a timestamp from the
/// flatbuffer. See the documentation of the individual member functions for
/// more information on how these should be implemented.
/// The second class which you must implement is a class that inherits from the
/// abstract class FileWriter::HDFWriterModule. This class should implement the
/// actual writing of flatbuffer data to the HDF5 file. More information on the
/// pure virtual functions that you must implement can be found below.

#pragma once
#include "../../FlatbufferReader.h"
#include "../../HDFWriterModule.h"
#include "NeXusDataset.h"

#include <iostream>

namespace NicosCacheWriter {

/// \brief This class is used to extract information from a flatbuffer which
/// uses a specific four character file identifier.
class CacheReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override;

  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override;

  uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override;

  std::string device_name(FileWriter::FlatbufferMessage const &Message) const;

  std::string
  parameter_name(FileWriter::FlatbufferMessage const &Message) const;

private:
  std::tuple<std::string, std::string, std::string>
  parse_nicos_key(FileWriter::FlatbufferMessage const &Message) const;
};

class CacheWriter : public FileWriter::HDFWriterModule {
public:
  CacheWriter() = default;
  ~CacheWriter() override = default;

  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override {
    std::cout << "CacheWriter::reopen()\n";
    return InitResult::OK;
  }

  void write(FileWriter::FlatbufferMessage const &Message) override;

  int32_t flush() override { return 0; }

  int32_t close() override;

protected:
  // void createHDFStructure(hdf5::node::Group &Group, size_t ChunkBytes);

  hdf5::Dimensions ChunkSize{64};
  // std::unique_ptr<NeXusDataset::MultiDimDatasetBase> Values;
  NeXusDataset::Time Timestamp;
  int CueInterval{1000};
  int CueCounter{0};
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace NicosCacheWriter
