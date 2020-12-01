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
#include "NDAr_Writer.h"
#include "WriterRegistrar.h"
#include <NDAr_NDArray_schema_generated.h>
#include <numeric>

namespace WriterModule {
namespace NDAr {

// Register the file writing part of this module.
static WriterModule::Registry::Registrar<NDAr_Writer>
    RegisterNDArWriter("NDAr", "NDAr");

std::uint64_t NDAr_Writer::epicsTimeToNsec(std::uint64_t sec,
                                           std::uint64_t nsec) {
  const auto TimeDiffUNIXtoEPICSepoch = 631152000L;
  const auto NSecMultiplier = 1000000000L;
  return (sec + TimeDiffUNIXtoEPICSepoch) * NSecMultiplier + nsec;
}

/// \brief Parse config JSON structure.
///
/// The default is to use double as the element type.
void NDAr_Writer::process_config() {
  std::map<std::string, NDAr_Writer::Type> TypeMap{
      {"int8", Type::int8},         {"uint8", Type::uint8},
      {"int16", Type::int16},       {"uint16", Type::uint16},
      {"int32", Type::int32},       {"uint32", Type::uint32},
      {"int64", Type::int64},       {"uint64", Type::uint64},
      {"float32", Type::float32},   {"float64", Type::float64},
      {"c_string", Type::c_string},
  };
  try {
    ElementType = TypeMap.at(DataType);
  } catch (std::out_of_range &E) {
    Logger->error("Unknown type ({}), using the default (double).",
                  DataType.getValue());
  }
}

WriterModule::InitResult NDAr_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  auto DefaultChunkSize = ChunkSize.operator hdf5::Dimensions().at(0);
  try {
    initValueDataset(HDFGroup);
    NeXusDataset::Time(             // NOLINT(bugprone-unused-raii)
        HDFGroup,                   // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(         // NOLINT(bugprone-unused-raii)
        HDFGroup,                   // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero( // NOLINT(bugprone-unused-raii)
        HDFGroup,                   // NOLINT(bugprone-unused-raii)
        NeXusDataset::Mode::Create, // NOLINT(bugprone-unused-raii)
        DefaultChunkSize);          // NOLINT(bugprone-unused-raii)
  } catch (std::exception &E) {
    Logger->error("Unable to initialise areaDetector data tree in "
                  "HDF file with error message: \"{}\"",
                  E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult NDAr_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    Values = std::make_unique<NeXusDataset::MultiDimDatasetBase>(
        HDFGroup, NeXusDataset::Mode::Open);
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
template <typename DataType, class DatasetType>
void appendData(DatasetType &Dataset, const std::uint8_t *Pointer, size_t Size,
                hdf5::Dimensions const &Shape) {
  Dataset->appendArray(
      ArrayAdapter<DataType>(reinterpret_cast<DataType *>(Pointer), Size),
      Shape);
}

void NDAr_Writer::write(const FileWriter::FlatbufferMessage &Message) {
  auto NDAr = FB_Tables::GetNDArray(Message.data());
  auto DataShape = hdf5::Dimensions(NDAr->dims()->begin(), NDAr->dims()->end());
  auto CurrentTimestamp =
      epicsTimeToNsec(NDAr->epicsTS()->secPastEpoch(), NDAr->epicsTS()->nsec());
  FB_Tables::DType Type = NDAr->dataType();
  auto DataPtr = NDAr->pData()->Data();
  auto NrOfElements =
      std::accumulate(std::cbegin(DataShape), std::cend(DataShape), size_t(1),
                      std::multiplies<>());

  switch (Type) {
  case FB_Tables::DType::Int8:
    appendData<const std::int8_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Uint8:
    appendData<const std::uint8_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Int16:
    appendData<const std::int16_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Uint16:
    appendData<const std::uint16_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Int32:
    appendData<const std::int32_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Uint32:
    appendData<const std::uint32_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Float32:
    appendData<const float>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::Float64:
    appendData<const double>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case FB_Tables::DType::c_string:
    appendData<const char>(Values, DataPtr, NrOfElements, DataShape);
    break;
  default:
    throw WriterModule::WriterException("Error in flatbuffer.");
  }
  Timestamp.appendElement(CurrentTimestamp);
  if (++CueCounter == CueInterval) {
    CueTimestampIndex.appendElement(Timestamp.dataspace().size() - 1);
    CueTimestamp.appendElement(CurrentTimestamp);
    CueCounter = 0;
  }
}

template <typename Type>
std::unique_ptr<NeXusDataset::MultiDimDatasetBase>
makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
       hdf5::Dimensions const &ChunkSize) {
  return std::make_unique<NeXusDataset::MultiDimDataset<Type>>(
      Parent, NeXusDataset::Mode::Create, Shape, ChunkSize);
}

void NDAr_Writer::initValueDataset(hdf5::node::Group &Parent) {
  using OpenFuncType =
      std::function<std::unique_ptr<NeXusDataset::MultiDimDatasetBase>()>;
  std::map<Type, OpenFuncType> CreateValuesMap{
      {Type::c_string,
       [&]() { return makeIt<char>(Parent, ArrayShape, ChunkSize); }},
      {Type::int8,
       [&]() { return makeIt<std::int8_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::uint8,
       [&]() { return makeIt<std::uint8_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::int16,
       [&]() { return makeIt<std::int16_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::uint16,
       [&]() { return makeIt<std::uint16_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::int32,
       [&]() { return makeIt<std::int32_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::uint32,
       [&]() { return makeIt<std::uint32_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::int64,
       [&]() { return makeIt<std::int64_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::uint64,
       [&]() { return makeIt<std::uint64_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::float32,
       [&]() { return makeIt<std::float_t>(Parent, ArrayShape, ChunkSize); }},
      {Type::float64,
       [&]() { return makeIt<std::double_t>(Parent, ArrayShape, ChunkSize); }},
  };
  Values = CreateValuesMap.at(ElementType)();
}
} // namespace NDAr
} // namespace WriterModule
