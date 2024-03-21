// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "f142_Writer.h"
#include "MetaData/HDF5DataWriter.h"
#include "WriterRegistrar.h"
#include "json.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <f142_logdata_generated.h>

namespace WriterModule::f142 {

using nlohmann::json;

using Type = f142_Writer::Type;

struct ValuesInformation {
  double Min{0};
  double Max{0};
  double Sum{0};
  uint64_t NrOfElements{0};
};

template <typename Type>
void makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
            hdf5::Dimensions const &ChunkSize) {
  NeXusDataset::MultiDimDataset<Type>( // NOLINT(bugprone-unused-raii)
      Parent, NeXusDataset::Mode::Create, Shape,
      ChunkSize); // NOLINT(bugprone-unused-raii)
}

void initValueDataset(hdf5::node::Group const &Parent, Type ElementType,
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
void f142_Writer::config_post_processing() {
  auto ToLower = [](auto InString) {
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
    ElementType = TypeMap.at(ToLower(DataType.getValue()));
  } catch (std::out_of_range &E) {
    LOG_WARN(R"(Unknown data type with name "{}". Using double.)",
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
    LOG_ERROR("f142 could not init_hdf hdf_parent: {}  trace: {}",
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
    LOG_ERROR(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

template <typename DataType, class DatasetType>
ValuesInformation appendData(DatasetType &Dataset, const void *Pointer,
                             size_t Size, bool GetArrayMetaData) {
  if (Size == 0) {
    return {};
  }
  auto DataArray = hdf5::ArrayAdapter<const DataType>(
      reinterpret_cast<DataType *>(Pointer), Size);
  Dataset.appendArray(DataArray, {Size});
  double Min{double(DataArray.data()[0])};
  double Max{double(DataArray.data()[0])};
  double Sum{double(DataArray.data()[0])};
  if (GetArrayMetaData) {
    for (auto i = 1u; i < Size; ++i) {
      Sum += double(DataArray.data()[i]);
      Min = std::min(Min, double(DataArray.data()[i]));
      Max = std::max(Max, double(DataArray.data()[i]));
    }
  }
  return {Min, Max, Sum, Size};
}

template <typename FBValueType, typename ReturnType>
ReturnType extractScalarValue(const LogData *LogDataMessage) {
  auto ScalarValue = LogDataMessage->value_as<FBValueType>();
  return ScalarValue->value();
}

template <typename DataType, typename ValueType, class DatasetType>
ValuesInformation appendScalarData(DatasetType &Dataset,
                                   const LogData *LogDataMessage) {
  auto ScalarValue = extractScalarValue<ValueType, DataType>(LogDataMessage);
  Dataset.appendArray(hdf5::ArrayAdapter<const DataType>(&ScalarValue, 1), {1});
  return {double(ScalarValue), double(ScalarValue), double(ScalarValue), 1};
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

void msgTypeIsConfigType(f142_Writer::Type ConfigType, Value MsgType) {
  std::unordered_map<Value, f142_Writer::Type> TypeComparison{
      {Value::ArrayByte, f142_Writer::Type::int8},
      {Value::Byte, f142_Writer::Type::int8},
      {Value::ArrayUByte, f142_Writer::Type::uint8},
      {Value::UByte, f142_Writer::Type::uint8},
      {Value::ArrayShort, f142_Writer::Type::int16},
      {Value::Short, f142_Writer::Type::int16},
      {Value::ArrayUShort, f142_Writer::Type::uint16},
      {Value::UShort, f142_Writer::Type::uint16},
      {Value::ArrayInt, f142_Writer::Type::int32},
      {Value::Int, f142_Writer::Type::int32},
      {Value::ArrayUInt, f142_Writer::Type::uint32},
      {Value::UInt, f142_Writer::Type::uint32},
      {Value::ArrayLong, f142_Writer::Type::int64},
      {Value::Long, f142_Writer::Type::int64},
      {Value::ArrayULong, f142_Writer::Type::uint64},
      {Value::ULong, f142_Writer::Type::uint64},
      {Value::ArrayFloat, f142_Writer::Type::float32},
      {Value::Float, f142_Writer::Type::float32},
      {Value::ArrayDouble, f142_Writer::Type::float64},
      {Value::Double, f142_Writer::Type::float64},
  };
  std::unordered_map<Value, std::string> MsgTypeString{
      {Value::ArrayByte, "int8"},      {Value::Byte, "int8"},
      {Value::ArrayUByte, "uint8"},    {Value::UByte, "uint8"},
      {Value::ArrayShort, "int16"},    {Value::Short, "int16"},
      {Value::ArrayUShort, "uint16"},  {Value::UShort, "uint16"},
      {Value::ArrayInt, "int32"},      {Value::Int, "int32"},
      {Value::ArrayUInt, "uint32"},    {Value::UInt, "uint32"},
      {Value::ArrayLong, "int64"},     {Value::Long, "int64"},
      {Value::ArrayULong, "uint64"},   {Value::ULong, "uint64"},
      {Value::ArrayFloat, "float32"},  {Value::Float, "float32"},
      {Value::ArrayDouble, "float64"}, {Value::Double, "float64"},
  };
  std::unordered_map<f142_Writer::Type, std::string> ConfigTypeString{
      {f142_Writer::Type::int8, "int8"},
      {f142_Writer::Type::uint8, "uint8"},
      {f142_Writer::Type::int16, "int16"},
      {f142_Writer::Type::uint16, "uint16"},
      {f142_Writer::Type::int32, "int32"},
      {f142_Writer::Type::uint32, "uint32"},
      {f142_Writer::Type::int64, "int64"},
      {f142_Writer::Type::uint64, "uint64"},
      {f142_Writer::Type::float32, "float32"},
      {f142_Writer::Type::float64, "float64"},
  };
  try {
    if (TypeComparison.at(MsgType) != ConfigType) {
      LOG_WARN("Configured data type ({}) is not the same as the f142 message "
               "type ({}).",
               ConfigTypeString.at(ConfigType), MsgTypeString.at(MsgType));
    }
  } catch (std::out_of_range const &) {
    LOG_ERROR("Got out of range error when comparing types.");
  }
}

void f142_Writer::writeImpl(FlatbufferMessage const &Message) {
  auto LogDataMessage = GetLogData(Message.data());
  size_t NrOfElements{1};
  Timestamp.appendElement(LogDataMessage->timestamp());
  auto Type = LogDataMessage->value_type();

  if (not HasCheckedMessageType) {
    msgTypeIsConfigType(ElementType, Type);
    HasCheckedMessageType = true;
  }

  // Note that we are using our knowledge about flatbuffers here to minimise
  // amount of code we have to write by using some pointer arithmetic.
  auto DataPtr = reinterpret_cast<void const *>(
      reinterpret_cast<uint8_t const *>(LogDataMessage->value()) + 4);

  auto extractArrayInfo = [&NrOfElements, &DataPtr]() {
    NrOfElements = *(reinterpret_cast<int const *>(DataPtr) + 1);
    DataPtr = reinterpret_cast<void const *>(
        reinterpret_cast<int const *>(DataPtr) + 2);
  };

  ValuesInformation CValuesInfo;
  switch (Type) {
  case Value::ArrayByte:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int8_t>(Values, DataPtr, NrOfElements,
                                                MetaData.getValue());
    break;
  case Value::Byte:
    CValuesInfo =
        appendScalarData<const std::int8_t, Byte>(Values, LogDataMessage);
    break;
  case Value::ArrayUByte:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint8_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::UByte:
    CValuesInfo =
        appendScalarData<const std::uint8_t, UByte>(Values, LogDataMessage);
    break;
  case Value::ArrayShort:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int16_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::Short:
    CValuesInfo =
        appendScalarData<const std::int16_t, Short>(Values, LogDataMessage);
    break;
  case Value::ArrayUShort:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint16_t>(Values, DataPtr, NrOfElements,
                                                  MetaData.getValue());
    break;
  case Value::UShort:
    CValuesInfo =
        appendScalarData<const std::uint16_t, UShort>(Values, LogDataMessage);
    break;
  case Value::ArrayInt:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int32_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::Int:
    CValuesInfo =
        appendScalarData<const std::int32_t, Int>(Values, LogDataMessage);
    break;
  case Value::ArrayUInt:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint32_t>(Values, DataPtr, NrOfElements,
                                                  MetaData.getValue());
    break;
  case Value::UInt:
    CValuesInfo =
        appendScalarData<const std::uint32_t, UInt>(Values, LogDataMessage);
    break;
  case Value::ArrayLong:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int64_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::Long:
    CValuesInfo =
        appendScalarData<const std::int64_t, Long>(Values, LogDataMessage);
    break;
  case Value::ArrayULong:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint64_t>(Values, DataPtr, NrOfElements,
                                                  MetaData.getValue());
    break;
  case Value::ULong:
    CValuesInfo =
        appendScalarData<const std::uint64_t, ULong>(Values, LogDataMessage);
    break;
  case Value::ArrayFloat:
    extractArrayInfo();
    CValuesInfo = appendData<const float>(Values, DataPtr, NrOfElements,
                                          MetaData.getValue());
    break;
  case Value::Float:
    CValuesInfo = appendScalarData<const float, Float>(Values, LogDataMessage);
    break;
  case Value::ArrayDouble:
    extractArrayInfo();
    CValuesInfo = appendData<const double>(Values, DataPtr, NrOfElements,
                                           MetaData.getValue());
    break;
  case Value::Double:
    CValuesInfo =
        appendScalarData<const double, Double>(Values, LogDataMessage);
    break;
  default:
    throw WriterModule::WriterException(
        "Unknown data type in f142 flatbuffer.");
  }

  ++NrOfWrites;
  if ((NrOfWrites - LastIndexAtWrite) / ValueIndexInterval.getValue() > 0) {
    LastIndexAtWrite = NrOfWrites;
    CueIndex.appendElement(NrOfWrites - 1);
    CueTimestampZero.appendElement(LogDataMessage->timestamp());
  }
  if (MetaData.getValue()) {
    if (TotalNrOfElementsWritten == 0) {
      Min = CValuesInfo.Min;
      Max = CValuesInfo.Max;
    }
    Min = std::min(Min, CValuesInfo.Min);
    Max = std::max(Max, CValuesInfo.Max);
    Sum += CValuesInfo.Sum;
    TotalNrOfElementsWritten += CValuesInfo.NrOfElements;
    MetaDataMin.setValue(Min);
    MetaDataMax.setValue(Max);
    MetaDataMean.setValue(Sum / TotalNrOfElementsWritten);
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

    auto Severity = static_cast<std::int16_t>(LogDataMessage->severity());
    AlarmSeverity.appendElement(F142SeverityToAl00Severity[Severity]);
  }
}

void f142_Writer::register_meta_data(hdf5::node::Group const &HDFGroup,
                                     const MetaData::TrackerPtr &Tracker) {

  if (MetaData.getValue()) {
    MetaDataMin = MetaData::Value<double>(HDFGroup, "minimum_value",
                                          MetaData::basicDatasetWriter<double>);
    Tracker->registerMetaData(MetaDataMin);

    MetaDataMax = MetaData::Value<double>(HDFGroup, "maximum_value",
                                          MetaData::basicDatasetWriter<double>);
    Tracker->registerMetaData(MetaDataMax);

    MetaDataMean = MetaData::Value<double>(
        HDFGroup, "average_value", MetaData::basicDatasetWriter<double>);
    Tracker->registerMetaData(MetaDataMean);
  }
}

/// Register the writer module.
static WriterModule::Registry::Registrar<f142_Writer> RegisterWriter("f142",
                                                                     "f142");

} // namespace WriterModule::f142
