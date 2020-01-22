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
#include "HDFFile.h"
#include <ns10_cache_entry_generated.h>

namespace Module {
namespace ns10 {

void ns10_Writer::parse_config(std::string const &ConfigurationStream) {
  auto Config = nlohmann::json::parse(ConfigurationStream);
  try {
    CueInterval = Config["cue_interval"].get<uint64_t>();
  } catch (std::exception const &Exception) {
    Logger->warn("Unable to extract cue interval from JSON.");
  }

  Logger->info("Using a cue interval of {}.", CueInterval);

  auto JsonChunkSize = Config["chunk_size"];
  if (JsonChunkSize.is_array()) {
    ChunkSize = Config["chunk_size"].get<hdf5::Dimensions>();
  } else if (JsonChunkSize.is_number_integer()) {
    ChunkSize = hdf5::Dimensions{JsonChunkSize.get<hsize_t>()};
  } else {
    Logger->warn("Unable to extract chunk size, using the existing value");
  }
  try {
    Sourcename = Config["source"];
  } catch (std::exception const &Exception) {
    Logger->error("Key \"source\" is not specified in JSON command");
  }
}

using FileWriterBase = Module::WriterBase;

Module::InitResult
ns10_Writer::init_hdf(hdf5::node::Group &HDFGroup,
                      std::string const &HDFAttributes) {
  const int DefaultChunkSize = ChunkSize.at(0);
  try {
    auto &CurrentGroup = HDFGroup;
    NeXusDataset::DoubleValue(      // NOLINT(bugprone-unused-raii)
        CurrentGroup,               // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
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
    FileWriter::writeAttributes(HDFGroup, &AttributesJson, Logger);
  } catch (std::exception &E) {
    Logger->error("Unable to initialise areaDetector data tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return Module::InitResult::ERROR;
  }
  return Module::InitResult::OK;
}

Module::InitResult ns10_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Values = NeXusDataset::DoubleValue(CurrentGroup, NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return Module::InitResult::ERROR;
  }
  return Module::InitResult::OK;
}

static CacheEntry const *getRoot(char const *Data) {
  return GetCacheEntry(Data);
}

void ns10_Writer::write(const FileWriter::FlatbufferMessage &Message) {
  auto Entry = getRoot(Message.data());
  auto CurrentTimestamp = Entry->time();
  auto Source = Entry->key();
  auto Value = Entry->value();
  if (!Source || !Value) {
    throw std::runtime_error("Invalid Flatbuffer content.");
  }

  if (Source->str() != Sourcename) {
    Logger->warn("Invalid source name: {}", Source->str());
    return;
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
} // namespace Module
