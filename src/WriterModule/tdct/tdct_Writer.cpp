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

#include "tdct_Writer.h"
#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include <limits>
#include <nlohmann/json.hpp>
#include <tdct_timestamps_generated.h>

namespace WriterModule {
namespace tdct {

// Register the file writing part of this module
static WriterModule::Registry::Registrar<tdct_Writer>
    RegisterSenvWriter("tdct", "tdct");

void tdct_Writer::process_config() {}

WriterModule::InitResult tdct_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
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
    Logger->error("Unable to initialise chopper time stamp tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult tdct_Writer::reopen(hdf5::node::Group &HDFGroup) {
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
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

void tdct_Writer::write(const FileWriter::FlatbufferMessage &Message) {
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

} // namespace tdct
} // namespace WriterModule
