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
/// \brief Implement classes required for writing chopper time stamps.

#include "ChopperTimeStampWriter.h"
#include "HDFFile.h"
#include "tdct_timestamps_generated.h"
#include <limits>
#include <nlohmann/json.hpp>

namespace tdct {

// Register the timestamp and name extraction class for this module
static FileWriter::FlatbufferReaderRegistry::Registrar<ChopperTimeStampGuard>
    RegisterSenvGuard("tdct");

// Register the file writing part of this module
static FileWriter::HDFWriterModuleRegistry::Registrar<ChopperTimeStampWriter>
    RegisterSenvWriter("tdct");

bool ChopperTimeStampGuard::verify(FlatbufferMessage const &Message) const {
  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const std::uint8_t *>(Message.data()), Message.size());
  return VerifytimestampBuffer(Verifier);
}

uint64_t
ChopperTimeStampGuard::timestamp(FlatbufferMessage const &Message) const {
  auto FbPointer = Gettimestamp(Message.data());
  if (FbPointer->timestamps()->size() == 0) {
    throw std::runtime_error(
        "Can not extract timestamp when timestamp array has zero elements.");
  }
  return FbPointer->timestamps()->operator[](0);
}

std::string ChopperTimeStampGuard::source_name(
    const FileWriter::FlatbufferMessage &Message) const {
  auto FbPointer = Gettimestamp(Message.data());
  return FbPointer->name()->str();
}

void ChopperTimeStampWriter::parse_config(std::string const &) {
  Logger->trace("There are currently no runtime configurable options in the "
                "ChopperTimeStampWriter class.");
}

FileWriterBase::InitResult
ChopperTimeStampWriter::init_hdf(hdf5::node::Group &HDFGroup,
                                 std::string const &HDFAttributes) {
  const int DefaultChunkSize = 1024;
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::Time(             // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(         // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero( // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    auto ClassAttribute =
        CurrentGroup.attributes.create<std::string>("NX_class");
    ClassAttribute.write("NXlog");
    auto AttributesJson = nlohmann::json::parse(HDFAttributes);
    FileWriter::writeAttributes(HDFGroup, &AttributesJson, SharedLogger());
  } catch (std::exception &E) {
    Logger->error("Unable to initialise chopper time stamp tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return HDFWriterModule::InitResult::ERROR;
  }
  return FileWriterBase::InitResult::OK;
}

FileWriterBase::InitResult
ChopperTimeStampWriter::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return HDFWriterModule::InitResult::ERROR;
  }
  return FileWriterBase::InitResult::OK;
}

void ChopperTimeStampWriter::write(
    const FileWriter::FlatbufferMessage &Message) {
  auto FbPointer = Gettimestamp(Message.data());
  auto TempTimePtr = FbPointer->timestamps()->data();
  auto TempTimeSize = FbPointer->timestamps()->size();
  if (TempTimeSize == 0) {
    Logger->warn(
        "Received a flatbuffer with zero (0) timestamps elements in it.");
    return;
  }
  ArrayAdapter<const std::uint64_t> CArray(TempTimePtr, TempTimeSize);
  auto CueIndexValue = Timestamp.dataspace().size();
  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(CueIndexValue));
  CueTimestamp.appendElement(FbPointer->timestamps()->operator[](0));
  Timestamp.appendArray(CArray);
}

std::int32_t ChopperTimeStampWriter::close() { return 0; }

} // namespace tdct
