// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/** Copyright (C) 2021 European Spallation Source ERIC */

/// \file
/// \brief Implement classes required to implement the ADC file writing module.

#include "helper.h"

#include "HDFOperations.h"
#include "WriterRegistrar.h"
#include "ad00_Writer.h"
#include <ad00_area_detector_array_generated.h>

namespace WriterModule::ad00 {

// Register the file writing part of this module.
static WriterModule::Registry::Registrar<ad00_Writer>
    RegisterNDArWriter("ad00", "ad00");

/// \brief Parse config JSON structure.
///
/// The default is to use double as the element type.
void ad00_Writer::config_post_processing() {
  std::map<std::string, ad00_Writer::Type> TypeMap{
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
    Logger::Error("Unknown type ({}), using the default (double).",
                  DataType.get_value());
  }
}

InitResult ad00_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
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
    HDFGroup["value"].attributes.create_from<std::string>("units", "");
  } catch (std::exception &E) {
    Logger::Error(
        R"(Unable to initialise areaDetector data tree in HDF file with error message: "{}")",
        E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult ad00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    Values = std::make_unique<NeXusDataset::MultiDimDatasetBase>(
        HDFGroup, "value", NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(HDFGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(HDFGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(HDFGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger::Error(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}
template <typename DataType, class DatasetType>
void appendData(DatasetType &Dataset, const std::uint8_t *Pointer, size_t Size,
                hdf5::Dimensions const &Shape) {
  Dataset->appendArray(
      hdf5::ArrayAdapter<DataType>(reinterpret_cast<DataType *>(Pointer), Size),
      Shape);
}

void msgTypeIsConfigType(ad00_Writer::Type ConfigType, DType MsgType) {
  std::unordered_map<DType, ad00_Writer::Type> TypeComparison{
      {DType::int8, ad00_Writer::Type::int8},
      {DType::uint8, ad00_Writer::Type::uint8},
      {DType::int16, ad00_Writer::Type::int16},
      {DType::uint16, ad00_Writer::Type::uint16},
      {DType::int32, ad00_Writer::Type::int32},
      {DType::uint32, ad00_Writer::Type::uint32},
      {DType::int64, ad00_Writer::Type::int64},
      {DType::uint64, ad00_Writer::Type::uint64},
      {DType::float32, ad00_Writer::Type::float32},
      {DType::float64, ad00_Writer::Type::float64},
  };
  std::unordered_map<DType, std::string> MsgTypeString{
      {DType::int8, "int8"},       {DType::uint8, "uint8"},
      {DType::int16, "int16"},     {DType::uint16, "uint16"},
      {DType::int32, "int32"},     {DType::uint32, "uint32"},
      {DType::int64, "int64"},     {DType::uint64, "uint64"},
      {DType::float32, "float32"}, {DType::float64, "float64"},
  };
  std::unordered_map<ad00_Writer::Type, std::string> ConfigTypeString{
      {ad00_Writer::Type::int8, "int8"},
      {ad00_Writer::Type::uint8, "uint8"},
      {ad00_Writer::Type::int16, "int16"},
      {ad00_Writer::Type::uint16, "uint16"},
      {ad00_Writer::Type::int32, "int32"},
      {ad00_Writer::Type::uint32, "uint32"},
      {ad00_Writer::Type::int64, "int64"},
      {ad00_Writer::Type::uint64, "uint64"},
      {ad00_Writer::Type::float32, "float32"},
      {ad00_Writer::Type::float64, "float64"}};
  try {
    if (TypeComparison.at(MsgType) != ConfigType) {
      Logger::Info(
          "Configured data type ({}) is not the same as the ad00 message "
          "type ({}).",
          ConfigTypeString.at(ConfigType), MsgTypeString.at(MsgType));
    }
  } catch (std::out_of_range const &) {
    Logger::Error("Got out of range error when comparing types.");
  }
}

bool ad00_Writer::writeImpl(const FileWriter::FlatbufferMessage &Message,
                            [[maybe_unused]] bool is_buffered_message) {
  auto ad00 = Getad00_ADArray(Message.data());
  auto DataShape =
      hdf5::Dimensions(ad00->dimensions()->begin(), ad00->dimensions()->end());
  auto CurrentTimestamp = ad00->timestamp();
  DType Type = ad00->data_type();

  if (!HasCheckedMessageType) {
    msgTypeIsConfigType(ElementType, Type);
    HasCheckedMessageType = true;
  }

  auto DataPtr = ad00->data()->Data();
  auto NrOfElements =
      std::accumulate(std::cbegin(DataShape), std::cend(DataShape), size_t(1),
                      std::multiplies<>());

  switch (Type) {
  case DType::int8:
    appendData<const std::int8_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::uint8:
    appendData<const std::uint8_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::int16:
    appendData<const std::int16_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::uint16:
    appendData<const std::uint16_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::int32:
    appendData<const std::int32_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::uint32:
    appendData<const std::uint32_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::int64:
    appendData<const std::int64_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::uint64:
    appendData<const std::uint64_t>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::float32:
    appendData<const float>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::float64:
    appendData<const double>(Values, DataPtr, NrOfElements, DataShape);
    break;
  case DType::c_string:
    appendData<const char>(Values, DataPtr, NrOfElements, DataShape);
    break;
  default:
    throw WriterModule::WriterException("Error in flatbuffer.");
  }
  Timestamp.appendElement(CurrentTimestamp);
  if (++CueCounter == CueInterval) {
    CueTimestampIndex.appendElement(Timestamp.current_size() - 1);
    CueTimestamp.appendElement(CurrentTimestamp);
    CueCounter = 0;
  }
  return true;
}

template <typename Type>
std::unique_ptr<NeXusDataset::MultiDimDatasetBase>
makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
       hdf5::Dimensions const &ChunkSize) {
  return std::make_unique<NeXusDataset::MultiDimDataset<Type>>(
      Parent, "value", NeXusDataset::Mode::Create, Shape, ChunkSize);
}

void ad00_Writer::initValueDataset(hdf5::node::Group const &Parent) const {
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
  CreateValuesMap.at(ElementType)();
}
} // namespace WriterModule::ad00
