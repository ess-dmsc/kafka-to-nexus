// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "ns10_Writer.h"
#include "FlatbufferMessage.h"
#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include <ns10_cache_entry_generated.h>

namespace WriterModule {
namespace ns10 {

static WriterModule::Registry::Registrar<ns10_Writer> RegisterWriter("ns10",
                                                                     "ns10");

void ns10_Writer::process_config() {}

using FileWriterBase = WriterModule::Base;

WriterModule::InitResult ns10_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::DoubleValue(      // NOLINT(bugprone-unused-raii)
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
    Logger->error("Unable to initialise areaDetector data tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult ns10_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    Values = NeXusDataset::DoubleValue(HDFGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(HDFGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(HDFGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(HDFGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

void ns10_Writer::write(const FileWriter::FlatbufferMessage &Message) {
  auto Entry = GetCacheEntry(Message.data());
  auto CurrentTimestamp = Entry->time();
  auto Source = Entry->key();
  auto Value = Entry->value();
  if (!Source || !Value) {
    throw std::runtime_error("Invalid Flatbuffer content.");
  }

  try {
    double ConvertedValue = std::stod(Value->str());
    Values.appendElement(ConvertedValue);
  } catch (std::invalid_argument const &Exception) {
    Logger->error("Could not convert string value to double: '{}'",
                  Value->str());
    throw;
  } catch (std::out_of_range const &Exception) {
    Logger->error("Converted value too big for result type: {}", Value->str());
    throw;
  }

  Timestamp.appendElement(std::lround(1e9 * CurrentTimestamp));
  if (++CueCounter == CueInterval) {
    CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestamp.appendElement(CurrentTimestamp);
    CueCounter = 0;
  }
}

} // namespace ns10
} // namespace WriterModule
