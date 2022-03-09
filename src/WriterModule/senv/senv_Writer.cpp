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
#include <senv_data_generated.h>

namespace WriterModule {
namespace senv {

// Register the file writing part of this module
static WriterModule::Registry::Registrar<senv_Writer>
    RegisterSenvWriter("senv", "senv");

WriterModule::InitResult
senv_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
  try {
    initValueDataset(HDFGroup);
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
    LOG_ERROR(R"(Unable to initialise fast sample environment data tree in HDF file with error message: "{}")",
              E.what());
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

WriterModule::InitResult senv_Writer::reopen(hdf5::node::Group &HDFGroup) {
  try {
    auto &CurrentGroup = HDFGroup;
    Value = NeXusDataset::ExtensibleDatasetBase(CurrentGroup, "value",
                                                NeXusDataset::Mode::Open);
    Timestamp = NeXusDataset::Time(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestampIndex =
        NeXusDataset::CueIndex(CurrentGroup, NeXusDataset::Mode::Open);
    CueTimestamp =
        NeXusDataset::CueTimestampZero(CurrentGroup, NeXusDataset::Mode::Open);
  } catch (std::exception &E) {
    LOG_ERROR(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return WriterModule::InitResult::ERROR;
  }
  return WriterModule::InitResult::OK;
}

void senv_Writer::config_post_processing() {
  std::map<std::string, senv_Writer::Type> TypeMap{
      {"int8", Type::int8},   {"uint8", Type::uint8},
      {"int16", Type::int16}, {"uint16", Type::uint16},
      {"int32", Type::int32}, {"uint32", Type::uint32},
      {"int64", Type::int64}, {"uint64", Type::uint64},
  };
  try {
    ElementType = TypeMap.at(DataType);
  } catch (std::out_of_range &E) {
    LOG_ERROR("Unknown type ({}), using the default (int64).",
              DataType.getValue());
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

void msgTypeIsConfigType(senv_Writer::Type ConfigType, ValueUnion MsgType) {
  std::unordered_map<ValueUnion, senv_Writer::Type> TypeComparison{
      {ValueUnion::Int8Array, senv_Writer::Type::int8},
      {ValueUnion::UInt8Array, senv_Writer::Type::uint8},
      {ValueUnion::Int16Array, senv_Writer::Type::int16},
      {ValueUnion::UInt16Array, senv_Writer::Type::uint16},
      {ValueUnion::Int32Array, senv_Writer::Type::int32},
      {ValueUnion::UInt32Array, senv_Writer::Type::uint32},
      {ValueUnion::Int64Array, senv_Writer::Type::int64},
      {ValueUnion::UInt64Array, senv_Writer::Type::uint64},
  };
  std::unordered_map<ValueUnion, std::string> MsgTypeString{
      {ValueUnion::Int8Array, "int8"},   {ValueUnion::UInt8Array, "uint8"},
      {ValueUnion::Int16Array, "int16"}, {ValueUnion::UInt16Array, "uint16"},
      {ValueUnion::Int32Array, "int32"}, {ValueUnion::UInt32Array, "uint32"},
      {ValueUnion::Int64Array, "int64"}, {ValueUnion::UInt64Array, "uint64"},
  };
  std::unordered_map<senv_Writer::Type, std::string> ConfigTypeString{
      {senv_Writer::Type::int8, "int8"},
      {senv_Writer::Type::uint8, "uint8"},
      {senv_Writer::Type::int16, "int16"},
      {senv_Writer::Type::uint16, "uint16"},
      {senv_Writer::Type::int32, "int32"},
      {senv_Writer::Type::uint32, "uint32"},
      {senv_Writer::Type::int64, "int64"},
      {senv_Writer::Type::uint64, "uint64"}};
  try {
    if (TypeComparison.at(MsgType) != ConfigType) {
      LOG_WARN("Configured data type ({}) is not the same as the senv message "
               "type ({}).",
               ConfigTypeString.at(ConfigType), MsgTypeString.at(MsgType));
    }
  } catch (std::out_of_range const &) {
    LOG_ERROR("Got out of range error when comparing types.");
  }
}

void senv_Writer::write(const FileWriter::FlatbufferMessage &Message) {
  auto FbPointer = GetSampleEnvironmentData(Message.data());
  auto CueIndexValue = Value.dataspace().size();
  auto ValuesType = FbPointer->Values_type();

  if (not HasCheckedMessageType) {
    msgTypeIsConfigType(ElementType, ValuesType);
    HasCheckedMessageType = true;
  }

  size_t NrOfElements{0};
  switch (ValuesType) {
  case ValueUnion::Int8Array: {
    auto ValuePtr = FbPointer->Values_as_Int8Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
    break;
  }
  case ValueUnion::UInt8Array: {
    auto ValuePtr = FbPointer->Values_as_UInt8Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::Int16Array: {
    auto ValuePtr = FbPointer->Values_as_Int16Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::UInt16Array: {
    auto ValuePtr = FbPointer->Values_as_UInt16Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::Int32Array: {
    auto ValuePtr = FbPointer->Values_as_Int32Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::UInt32Array: {
    auto ValuePtr = FbPointer->Values_as_UInt32Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::Int64Array: {
    auto ValuePtr = FbPointer->Values_as_Int64Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  case ValueUnion::UInt64Array: {
    auto ValuePtr = FbPointer->Values_as_UInt64Array()->value();
    NrOfElements = ValuePtr->size();
    Value.appendArray(hdf5::ArrayAdapter(ValuePtr->data(), NrOfElements));
  } break;
  default:
    LOG_WARN("Unknown data type in flatbuffer.");
  }
  if (NrOfElements == 0) {
    return;
  }
  CueTimestampIndex.appendElement(static_cast<std::uint32_t>(CueIndexValue));
  CueTimestamp.appendElement(FbPointer->PacketTimestamp());

  // Time-stamps are available in the flatbuffer
  if (flatbuffers::IsFieldPresent(FbPointer,
                                  SampleEnvironmentData::VT_TIMESTAMPS)) {
    auto TimestampPtr = FbPointer->Timestamps()->data();
    auto TimestampSize = FbPointer->Timestamps()->size();
    hdf5::ArrayAdapter<const std::uint64_t> TSArray(TimestampPtr,
                                                    TimestampSize);
    Timestamp.appendArray(TSArray);
  } else { // If timestamps are not available, generate them
    std::vector<std::uint64_t> TempTimeStamps(GenerateTimeStamps(
        FbPointer->PacketTimestamp(), FbPointer->TimeDelta(), NrOfElements));
    Timestamp.appendArray(TempTimeStamps);
  }
}

template <typename Type>
std::unique_ptr<hdf5::node::ChunkedDataset>
makeIt(hdf5::node::Group const &Parent, size_t const &ChunkSize) {
  return std::make_unique<NeXusDataset::ExtensibleDataset<Type>>(
      Parent, "value", NeXusDataset::Mode::Create, ChunkSize);
}

void senv_Writer::initValueDataset(hdf5::node::Group const &Parent) const {
  using OpenFuncType =
      std::function<std::unique_ptr<hdf5::node::ChunkedDataset>()>;
  std::map<Type, OpenFuncType> CreateValuesMap{
      {Type::int8, [&]() { return makeIt<std::int8_t>(Parent, ChunkSize); }},
      {Type::uint8, [&]() { return makeIt<std::uint8_t>(Parent, ChunkSize); }},
      {Type::int16, [&]() { return makeIt<std::int16_t>(Parent, ChunkSize); }},
      {Type::uint16,
       [&]() { return makeIt<std::uint16_t>(Parent, ChunkSize); }},
      {Type::int32, [&]() { return makeIt<std::int32_t>(Parent, ChunkSize); }},
      {Type::uint32,
       [&]() { return makeIt<std::uint32_t>(Parent, ChunkSize); }},
      {Type::int64, [&]() { return makeIt<std::int64_t>(Parent, ChunkSize); }},
      {Type::uint64,
       [&]() { return makeIt<std::uint64_t>(Parent, ChunkSize); }},
  };
  CreateValuesMap.at(ElementType)();
}

} // namespace senv
} // namespace WriterModule
