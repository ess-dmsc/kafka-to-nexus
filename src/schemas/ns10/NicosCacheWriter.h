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

class StringValue : public NeXusDataset::ExtensibleDataset<std::string> {
public:
  StringValue() = default;
  /// \brief Create the string_value dataset of NXLog.
  /// \throw std::runtime_error if dataset already exists.
  StringValue(hdf5::node::Group const &Parent, NeXusDataset::Mode CMode,
              size_t ChunkSize = 1024);
};

/// \brief This class is used to extract information from a flatbuffer which
/// uses a specific four character file identifier.
class CacheReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &Message) const override;

  std::string
  source_name(FileWriter::FlatbufferMessage const &Message) const override;

  uint64_t
  timestamp(FileWriter::FlatbufferMessage const &Message) const override;

private:
};

class CacheWriter : public FileWriter::HDFWriterModule {
public:
  CacheWriter() = default;
  ~CacheWriter() override = default;

  void parse_config(std::string const &ConfigurationStream,
                    std::string const &ConfigurationModule) override;

  InitResult init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) override;

  InitResult reopen(hdf5::node::Group &HDFGroup) override;

  void write(FileWriter::FlatbufferMessage const &Message) override;

  int32_t flush() override;

  int32_t close() override;

protected:
  void initValueDataset(hdf5::node::Group &Parent);

  std::string Sourcename;
  StringValue Values;
  hdf5::Dimensions ChunkSize{64};
  NeXusDataset::Time Timestamp;
  int CueInterval{1000};
  int CueCounter{0};
  NeXusDataset::CueIndex CueTimestampIndex;
  NeXusDataset::CueTimestampZero CueTimestamp;

private:
  SharedLogger Logger = spdlog::get("filewriterlogger");
};
} // namespace NicosCacheWriter
