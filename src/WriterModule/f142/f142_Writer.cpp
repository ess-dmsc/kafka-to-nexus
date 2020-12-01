// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "f142_Writer.h"
#include "WriterRegistrar.h"
#include "json.h"
#include <algorithm>
#include <cctype>
#include <f142_logdata_generated.h>

namespace WriterModule {
namespace f142 {

using nlohmann::json;

using Type = f142_Writer::Type;

template <typename Type>
void makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
            hdf5::Dimensions const &ChunkSize) {
  NeXusDataset::MultiDimDataset<Type>( // NOLINT(bugprone-unused-raii)
      Parent, NeXusDataset::Mode::Create, Shape,
      ChunkSize); // NOLINT(bugprone-unused-raii)
}

void initValueDataset(hdf5::node::Group &Parent, Type ElementType,
                      hdf5::Dimensions const &Shape,
                      hdf5::Dimensions const &ChunkSize) {
  using OpenFuncType = std::function<void()>;
  std::map<Type, OpenFuncType> CreateValuesMap{
      {Type::int8, [&]() { makeIt<std::int8_t>(Parent, Shape, ChunkSize); }},
      {Type::uint8, [&]() { makeIt<std::uint8_t>(Parent, Shape, ChunkSize); }},
      {Type::int16, [&]() { makeIt<std::int16_t>(Parent, Shape, ChunkSize); }},
      {Type::uint16,
       [&]() { makeIt<std::uint16_t>(Parent, Shape, ChunkSize); }},
      {Type::int32, [&]() { makeIt<std::int32_t>(Parent, Shape, ChunkSize); }},
      {Type::uint32,
       [&]() { makeIt<std::uint32_t>(Parent, Shape, ChunkSize); }},
      {Type::int64, [&]() { makeIt<std::int64_t>(Parent, Shape, ChunkSize); }},
      {Type::uint64,
       [&]() { makeIt<std::uint64_t>(Parent, Shape, ChunkSize); }},
      {Type::float32,
       [&]() { makeIt<std::float_t>(Parent, Shape, ChunkSize); }},
      {Type::float64,
       [&]() { makeIt<std::double_t>(Parent, Shape, ChunkSize); }},
  };
  CreateValuesMap.at(ElementType)();
}

/// Parse the configuration for this stream.
void f142_Writer::process_config() {
  auto toLower = [](auto InString) {
    std::transform(InString.begin(), InString.end(), InString.begin(),
                   [](auto C) { return std::tolower(C); });
    return InString;
  };
  std::map<std::string, Type> TypeMap{
      {"int8", Type::int8},       {"uint8", Type::uint8},
      {"int16", Type::int16},     {"uint16", Type::uint16},
      {"int32", Type::int32},     {"uint32", Type::uint32},
      {"int64", Type::int64},     {"uint64", Type::uint64},
      {"float32", Type::float32}, {"float64", Type::float64},
      {"float", Type::float32},   {"double", Type::float64},
      {"short", Type::int16},     {"int", Type::int32},
      {"long", Type::int64}};

  try {
    ElementType = TypeMap.at(toLower(DataType.getValue()));
  } catch (std::out_of_range &E) {
    Logger->warn("Unknown data type with name \"{}\". Using double.",
                 DataType.getValue());
  }
}

/// \brief Implement the writer module interface, forward to the CREATE case
/// of
/// `init_hdf`.
InitResult f142_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::Time(HDFGroup, Create,
                       ChunkSize); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero(HDFGroup, Create,
                                   ChunkSize); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(HDFGroup, Create,
                           ChunkSize); // NOLINT(bugprone-unused-raii)
    initValueDataset(HDFGroup, ElementType,
                     {
                         ArraySize,
                     },
                     {ChunkSize, ArraySize});

    NeXusDataset::AlarmTime(HDFGroup, Create);
    NeXusDataset::AlarmStatus(HDFGroup, Create);
    NeXusDataset::AlarmSeverity(HDFGroup, Create);
    if (not Unit.getValue().empty()) {
      HDFGroup["value"].attributes.create_from<std::string>("units", Unit);
    }

  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger->error("f142 could not init hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the writer module interface, forward to the OPEN case of
/// `init_hdf`.
InitResult f142_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    Timestamp = NeXusDataset::Time(HDFGroup, Open);
    CueIndex = NeXusDataset::CueIndex(HDFGroup, Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, Open);
    Values = NeXusDataset::MultiDimDatasetBase(HDFGroup, Open);
    AlarmTime = NeXusDataset::AlarmTime(HDFGroup, Open);
    AlarmStatus = NeXusDataset::AlarmStatus(HDFGroup, Open);
    AlarmSeverity = NeXusDataset::AlarmSeverity(HDFGroup, Open);
  } catch (std::exception &E) {
    Logger->error(
        "Failed to reopen datasets in HDF file with error message: \"{}\"",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

template <typename DataType, class DatasetType>
void appendData(DatasetType &Dataset, const void *Pointer, size_t Size) {
  Dataset.appendArray(
      ArrayAdapter<const DataType>(reinterpret_cast<DataType *>(Pointer), Size),
      {
          Size,
      });
}

template <typename FBValueType, typename ReturnType>
ReturnType extractScalarValue(const LogData *LogDataMessage) {
  auto ScalarValue = LogDataMessage->value_as<FBValueType>();
  return ScalarValue->value();
}

template <typename DataType, typename ValueType, class DatasetType>
void appendScalarData(DatasetType &Dataset, const LogData *LogDataMessage) {
  auto ScalarValue = extractScalarValue<ValueType, DataType>(LogDataMessage);
  Dataset.appendArray(ArrayAdapter<const DataType>(&ScalarValue, 1), {1});
}

std::unordered_map<AlarmStatus, std::string> AlarmStatusToString{
    {AlarmStatus::NO_ALARM, "NO_ALARM"},
    {AlarmStatus::WRITE_ACCESS, "WRITE_ACCESS"},
    {AlarmStatus::READ_ACCESS, "READ_ACCESS"},
    {AlarmStatus::READ, "READ"},
    {AlarmStatus::WRITE, "WRITE"},
    {AlarmStatus::HWLIMIT, "HWLIMIT"},
    {AlarmStatus::DISABLE, "DISABLE"},
    {AlarmStatus::BAD_SUB, "BAD_SUB"},
    {AlarmStatus::TIMED, "TIMED"},
    {AlarmStatus::SOFT, "SOFT"},
    {AlarmStatus::SIMM, "SIMM"},
    {AlarmStatus::LINK, "LINK"},
    {AlarmStatus::LOW, "LOW"},
    {AlarmStatus::LOLO, "LOLO"},
    {AlarmStatus::HIGH, "HIGH"},
    {AlarmStatus::HIHI, "HIHI"},
    {AlarmStatus::SCAN, "SCAN"},
    {AlarmStatus::STATE, "STATE"},
    {AlarmStatus::COS, "COS"},
    {AlarmStatus::UDF, "UDF"},
    {AlarmStatus::CALC, "CALC"},
    {AlarmStatus::COMM, "COMM"},
    {AlarmStatus::NO_CHANGE, "NO_CHANGE"}};

std::unordered_map<AlarmSeverity, std::string> AlarmSeverityToString{
    {AlarmSeverity::NO_ALARM, "NO_ALARM"},
    {AlarmSeverity::MINOR, "MINOR"},
    {AlarmSeverity::MAJOR, "MAJOR"},
    {AlarmSeverity::INVALID, "INVALID"},
    {AlarmSeverity::NO_CHANGE, "NO_CHANGE"}};

void f142_Writer::write(FlatbufferMessage const &Message) {
  auto LogDataMessage = GetLogData(Message.data());
  size_t NrOfElements{1};
  Timestamp.appendElement(LogDataMessage->timestamp());
  auto Type = LogDataMessage->value_type();

  // Note that we are using our knowledge about flatbuffers here to minimise
  // amount of code we have to write by using some pointer arithmetic.
  auto DataPtr = reinterpret_cast<void const *>(
      reinterpret_cast<uint8_t const *>(LogDataMessage->value()) + 4);

  auto extractArrayInfo = [&NrOfElements, &DataPtr]() {
    NrOfElements = *(reinterpret_cast<int const *>(DataPtr) + 1);
    DataPtr = reinterpret_cast<void const *>(
        reinterpret_cast<int const *>(DataPtr) + 2);
  };

  switch (Type) {
  case Value::ArrayByte:
    extractArrayInfo();
    appendData<const std::int8_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::Byte:
    appendScalarData<const std::int8_t, Byte>(Values, LogDataMessage);
    break;
  case Value::ArrayUByte:
    extractArrayInfo();
    appendData<const std::uint8_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::UByte:
    appendScalarData<const std::uint8_t, UByte>(Values, LogDataMessage);
    break;
  case Value::ArrayShort:
    extractArrayInfo();
    appendData<const std::int16_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::Short:
    appendScalarData<const std::int16_t, Short>(Values, LogDataMessage);
    break;
  case Value::ArrayUShort:
    extractArrayInfo();
    appendData<const std::uint16_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::UShort:
    appendScalarData<const std::uint16_t, UShort>(Values, LogDataMessage);
    break;
  case Value::ArrayInt:
    extractArrayInfo();
    appendData<const std::int32_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::Int:
    appendScalarData<const std::int32_t, Int>(Values, LogDataMessage);
    break;
  case Value::ArrayUInt:
    extractArrayInfo();
    appendData<const std::uint32_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::UInt:
    appendScalarData<const std::uint32_t, UInt>(Values, LogDataMessage);
    break;
  case Value::ArrayLong:
    extractArrayInfo();
    appendData<const std::int64_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::Long:
    appendScalarData<const std::int64_t, Long>(Values, LogDataMessage);
    break;
  case Value::ArrayULong:
    extractArrayInfo();
    appendData<const std::uint64_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ULong:
    appendScalarData<const std::uint64_t, ULong>(Values, LogDataMessage);
    break;
  case Value::ArrayFloat:
    extractArrayInfo();
    appendData<const float>(Values, DataPtr, NrOfElements);
    break;
  case Value::Float:
    appendScalarData<const float, Float>(Values, LogDataMessage);
    break;
  case Value::ArrayDouble:
    extractArrayInfo();
    appendData<const double>(Values, DataPtr, NrOfElements);
    break;
  case Value::Double:
    appendScalarData<const double, Double>(Values, LogDataMessage);
    break;
  default:
    throw WriterModule::WriterException(
        "Unknown data type in f142 flatbuffer.");
  }

  // AlarmStatus::NO_CHANGE is not a real EPICS alarm status value, it is used
  // by the Forwarder to indicate that the alarm has not changed from the
  // previously published value. The Filewriter only records changes in alarm
  // status.
  if (LogDataMessage->status() != AlarmStatus::NO_CHANGE) {
    AlarmTime.appendElement(LogDataMessage->timestamp());

    auto const AlarmStatusStringIterator =
        AlarmStatusToString.find(LogDataMessage->status());
    std::string AlarmStatusString = "UNRECOGNISED_STATUS";
    if (AlarmStatusStringIterator != AlarmStatusToString.end()) {
      AlarmStatusString = AlarmStatusStringIterator->second;
    }
    AlarmStatus.appendStringElement(AlarmStatusString);

    auto const AlarmSeverityStringIterator =
        AlarmSeverityToString.find(LogDataMessage->severity());
    std::string AlarmSeverityString = "UNRECOGNISED_SEVERITY";
    if (AlarmSeverityStringIterator != AlarmSeverityToString.end()) {
      AlarmSeverityString = AlarmSeverityStringIterator->second;
    }
    AlarmSeverity.appendStringElement(AlarmSeverityString);
  }
}
/// Register the writer module.
static WriterModule::Registry::Registrar<f142_Writer> RegisterWriter("f142",
                                                                     "f142");

} // namespace f142
} // namespace WriterModule
