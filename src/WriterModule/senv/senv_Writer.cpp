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
/// \brief Implement classes required to implement the ADC file writing module.

#include "helper.h"

#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include "senv_Writer.h"
#include <limits>
#include <senv_data_generated.h>

namespace WriterModule {
namespace senv {

// Register the file writing part of this module
static WriterModule::Registry::Registrar<senv_Writer>
    RegisterSenvWriter("senv", "senv");

WriterModule::InitResult senv_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::UInt16Value(      // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        ChunkSize);                 // NOLINT(bugprone-unused-raii)
    NeXusDataset::Time(             // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        ChunkSize);                 // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(         // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        ChunkSize);                 // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero( // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        ChunkSize);                 // NOLINT(bugprone-unused-raii)
  } catch (std::exception &E) {
    Logger->error("Unable to initialise fast sample environment data tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult senv_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Value = NeXusDataset::UInt16Value(CurrentGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

std::vector<std::uint64_t> GenerateTimeStamps(std::uint64_t OriginTimeStamp,
                                              double TimeDelta,
                                              int NumberOfElements) {
  std::vector<std::uint64_t> ReturnVector(NumberOfElements);
  for (int i = 0; i < NumberOfElements; i++) {
    ReturnVector[i] = OriginTimeStamp + std::llround(i * TimeDelta);
  }
  return ReturnVector;
}

void senv_Writer::write(const FileWriter::FlatbufferMessage &Message) {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  auto TempDataPtr = FbPointer->Values()->data();
  auto TempDataSize = FbPointer->Values()->size();
  if (TempDataSize == 0) {
    Logger->warn("Received a flatbuffer with zero (0) data elements in it.");
    return;
  }
  ArrayAdapter<const std::uint16_t> CArray(TempDataPtr, TempDataSize);
  auto CueIndexValue = Value.dataspace().size();
  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(CueIndexValue));
  CueTimestamp.appendElement(FbPointer->PacketTimestamp());
  Value.appendArray(CArray);
  // Time-stamps are available in the flatbuffer
  if (flatbuffers::IsFieldPresent(FbPointer,
                                  SampleEnvironmentData::VT_TIMESTAMPS) and
      FbPointer->Values()->size() == FbPointer->Timestamps()->size()) {
    auto TimestampPtr = FbPointer->Timestamps()->data();
    auto TimestampSize = FbPointer->Timestamps()->size();
    ArrayAdapter<const std::uint64_t> TSArray(TimestampPtr, TimestampSize);
    Timestamp.appendArray(TSArray);
  } else { // If timestamps are not available, generate them
    std::vector<std::uint64_t> TempTimeStamps(GenerateTimeStamps(
        FbPointer->PacketTimestamp(), FbPointer->TimeDelta(), TempDataSize));
    Timestamp.appendArray(TempTimeStamps);
  }
}

} // namespace senv
} // namespace WriterModule
