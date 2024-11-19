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
#include "se00_Writer.h"
#include <se00_data_generated.h>

namespace WriterModule::se00 {

// Register the file writing part of this module
static WriterModule::Registry::Registrar<se00_Writer>
    RegisterSenvWriter("se00", "se00");

WriterModule::InitResult se00_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  try {
    initValueDataset(HDFGroup);
    auto const &CurrentGroup = HDFGroup;
    Timestamp =
        NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Create, ChunkSize);
    CueTimestampIndex = NeXusDataset::CueIndex(
        CurrentGroup, NeXusDataset::Mode::Create, ChunkSize);
    CueTimestamp = NeXusDataset::CueTimestampZero(
        CurrentGroup, NeXusDataset::Mode::Create, ChunkSize);
  } catch (std::exception &E) {
    Logger::Error(
        R"(Unable to initialise fast sample environment data tree in HDF file with error message: "{}")",
        E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult se00_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Value = std::make_unique<NeXusDataset::ExtensibleDatasetBase>(
        CurrentGroup, "value", NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    Logger::Error(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

void se00_Writer::config_post_processing() {
  std::map<std::string, se00_Writer::Type> TypeMap{
      {"int8", Type::int8},     {"uint8", Type::uint8},
      {"int16", Type::int16},   {"uint16", Type::uint16},
      {"int32", Type::int32},   {"uint32", Type::uint32},
      {"int64", Type::int64},   {"uint64", Type::uint64},
      {"float", Type::float32}, {"double", Type::float64},
  };
  try {
    ElementType = TypeMap.at(DataType);
  } catch (std::out_of_range &E) {
    Logger::Error("Unknown type ({}), using the default (int64).",
                  DataType.get_value());
  }
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

void msgTypeIsConfigType(se00_Writer::Type ConfigType, ValueUnion MsgType) {
  std::unordered_map<ValueUnion, se00_Writer::Type> TypeComparison{
      {ValueUnion::Int8Array, se00_Writer::Type::int8},
      {ValueUnion::UInt8Array, se00_Writer::Type::uint8},
      {ValueUnion::Int16Array, se00_Writer::Type::int16},
      {ValueUnion::UInt16Array, se00_Writer::Type::uint16},
      {ValueUnion::Int32Array, se00_Writer::Type::int32},
      {ValueUnion::UInt32Array, se00_Writer::Type::uint32},
      {ValueUnion::Int64Array, se00_Writer::Type::int64},
      {ValueUnion::UInt64Array, se00_Writer::Type::uint64},
      {ValueUnion::FloatArray, se00_Writer::Type::float32},
      {ValueUnion::DoubleArray, se00_Writer::Type::float64},
  };
  std::unordered_map<ValueUnion, std::string> MsgTypeString{
      {ValueUnion::Int8Array, "int8"},     {ValueUnion::UInt8Array, "uint8"},
      {ValueUnion::Int16Array, "int16"},   {ValueUnion::UInt16Array, "uint16"},
      {ValueUnion::Int32Array, "int32"},   {ValueUnion::UInt32Array, "uint32"},
      {ValueUnion::Int64Array, "int64"},   {ValueUnion::UInt64Array, "uint64"},
      {ValueUnion::DoubleArray, "double"}, {ValueUnion::FloatArray, "float"},
  };
  std::unordered_map<se00_Writer::Type, std::string> ConfigTypeString{
      {se00_Writer::Type::int8, "int8"},
      {se00_Writer::Type::uint8, "uint8"},
      {se00_Writer::Type::int16, "int16"},
      {se00_Writer::Type::uint16, "uint16"},
      {se00_Writer::Type::int32, "int32"},
      {se00_Writer::Type::uint32, "uint32"},
      {se00_Writer::Type::int64, "int64"},
      {se00_Writer::Type::uint64, "uint64"},
      {se00_Writer::Type::float32, "float"},
      {se00_Writer::Type::float64, "double"}};
  try {
    if (TypeComparison.at(MsgType) != ConfigType) {
      Logger::Info(
          "Configured data type ({}) is not the same as the se00 message "
          "type ({}).",
          ConfigTypeString.at(ConfigType), MsgTypeString.at(MsgType));
    }
  } catch (std::out_of_range const &) {
    Logger::Error("Got out of range error when comparing types.");
  }
}

void se00_Writer::writeImpl(const FileWriter::FlatbufferMessage &Message, [[maybe_unused]] bool is_buffered_message) {
  auto FbPointer = Getse00_SampleEnvironmentData(Message.data());
  auto CueIndexValue = Value->current_size();
  auto ValuesType = FbPointer->values_type();

  if (not HasCheckedMessageType) {
    msgTypeIsConfigType(ElementType, ValuesType);
    HasCheckedMessageType = true;
  }

  size_t NrOfElements{0};
  switch (ValuesType) {
  case ValueUnion::Int8Array: {
    auto ValuePtr = FbPointer->values_as_Int8Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
    break;
  }
  case ValueUnion::UInt8Array: {
    auto ValuePtr = FbPointer->values_as_UInt8Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::Int16Array: {
    auto ValuePtr = FbPointer->values_as_Int16Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::UInt16Array: {
    auto ValuePtr = FbPointer->values_as_UInt16Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::Int32Array: {
    auto ValuePtr = FbPointer->values_as_Int32Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::UInt32Array: {
    auto ValuePtr = FbPointer->values_as_UInt32Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::Int64Array: {
    auto ValuePtr = FbPointer->values_as_Int64Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::UInt64Array: {
    auto ValuePtr = FbPointer->values_as_UInt64Array()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::FloatArray: {
    auto ValuePtr = FbPointer->values_as_FloatArray()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::DoubleArray: {
    auto ValuePtr = FbPointer->values_as_DoubleArray()->value();
    NrOfElements = ValuePtr->size();
    Value->appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  default:
    Logger::Info("Unknown data type in flatbuffer.");
  }
  if (NrOfElements == 0) {
    return;
  }
  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(CueIndexValue));
  CueTimestamp.appendElement(FbPointer->packet_timestamp());

  // Time-stamps are available in the flatbuffer
  if (flatbuffers::IsFieldPresent(FbPointer,
                                  se00_SampleEnvironmentData::VT_TIMESTAMPS)) {
    auto TimestampPtr = FbPointer->timestamps()->data();
    auto TimestampSize = FbPointer->timestamps()->size();
    hdf5::ArrayAdapter<const std::int64_t> TSArray(TimestampPtr, TimestampSize);
    Timestamp.appendArray(TSArray);
  } else { // If timestamps are not available, generate them
    std::vector<std::uint64_t> TempTimeStamps(GenerateTimeStamps(
        FbPointer->packet_timestamp(), FbPointer->time_delta(), NrOfElements));
    Timestamp.appendArray(TempTimeStamps);
  }
}

template <typename Type>
std::unique_ptr<NeXusDataset::ExtensibleDatasetBase>
makeIt(hdf5::node::Group const &Parent, size_t const &ChunkSize) {
  return std::make_unique<NeXusDataset::ExtensibleDataset<Type>>(
      Parent, "value", NeXusDataset::Mode::Create, ChunkSize);
}

void se00_Writer::initValueDataset(hdf5::node::Group const &Parent) {
  std::unique_ptr<NeXusDataset::ExtensibleDatasetBase> temporary = nullptr;
  switch (ElementType) {
  case Type::int8:
    temporary = makeIt<std::int8_t>(Parent, ChunkSize);
    break;
  case Type::uint8:
    temporary = makeIt<std::uint8_t>(Parent, ChunkSize);
    break;
  case Type::int16:
    temporary = makeIt<std::int16_t>(Parent, ChunkSize);
    break;
  case Type::uint16:
    temporary = makeIt<std::uint16_t>(Parent, ChunkSize);
    break;
  case Type::int32:
    temporary = makeIt<std::int32_t>(Parent, ChunkSize);
    break;
  case Type::uint32:
    temporary = makeIt<std::uint32_t>(Parent, ChunkSize);
    break;
  case Type::int64:
    temporary = makeIt<std::int64_t>(Parent, ChunkSize);
    break;
  case Type::uint64:
    temporary = makeIt<std::uint64_t>(Parent, ChunkSize);
    break;
  case Type::float32:
    temporary = makeIt<std::float_t>(Parent, ChunkSize);
    break;
  case Type::float64:
    temporary = makeIt<std::double_t>(Parent, ChunkSize);
    break;
  }
  Value.swap(temporary);
}

} // namespace WriterModule::se00
