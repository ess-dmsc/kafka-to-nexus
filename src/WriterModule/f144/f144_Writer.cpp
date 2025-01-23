// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "f144_Writer.h"
#include "MetaData/HDF5DataWriter.h"
#include "WriterRegistrar.h"
#include "json.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <f144_logdata_generated.h>

namespace WriterModule::f144 {

using nlohmann::json;

using Type = f144_Writer::Type;

struct ValuesInformation {
  double Min{0};
  double Max{0};
  double Sum{0};
  uint64_t NrOfElements{0};
};

template <typename Type> void makeIt(hdf5::node::Group const &Parent) {
  NeXusDataset::ExtensibleDataset<Type>( // NOLINT(bugprone-unused-raii)
      Parent, "value",
      NeXusDataset::Mode::Create); // NOLINT(bugprone-unused-raii)
}

void initValueDataset(hdf5::node::Group const &Parent, Type ElementType) {
  using OpenFuncType = std::function<void()>;
  std::map<Type, OpenFuncType> CreateValuesMap{
      {Type::int8, [&]() { makeIt<std::int8_t>(Parent); }},
      {Type::uint8, [&]() { makeIt<std::uint8_t>(Parent); }},
      {Type::int16, [&]() { makeIt<std::int16_t>(Parent); }},
      {Type::uint16, [&]() { makeIt<std::uint16_t>(Parent); }},
      {Type::int32, [&]() { makeIt<std::int32_t>(Parent); }},
      {Type::uint32, [&]() { makeIt<std::uint32_t>(Parent); }},
      {Type::int64, [&]() { makeIt<std::int64_t>(Parent); }},
      {Type::uint64, [&]() { makeIt<std::uint64_t>(Parent); }},
      {Type::float32, [&]() { makeIt<std::float_t>(Parent); }},
      {Type::float64, [&]() { makeIt<std::double_t>(Parent); }},
  };
  CreateValuesMap.at(ElementType)();
}

/// Parse the configuration for this stream.
void f144_Writer::config_post_processing() {
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
    ElementType = TypeMap.at(ToLower(DataType.get_value()));
  } catch (std::out_of_range &E) {
    Logger::Info(R"(Unknown data type with name "{}". Using double.)",
                 DataType.get_value());
  }
}

/// \brief Implement the writer module interface, forward to the CREATE case
/// of
/// `init_hdf`.
InitResult f144_Writer::init_hdf(hdf5::node::Group &HDFGroup) {
  auto Create = NeXusDataset::Mode::Create;
  try {
    NeXusDataset::Time(HDFGroup, Create,
                       ChunkSize); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueTimestampZero(HDFGroup, Create,
                                   ChunkSize); // NOLINT(bugprone-unused-raii)
    NeXusDataset::CueIndex(HDFGroup, Create,
                           ChunkSize); // NOLINT(bugprone-unused-raii)
    initValueDataset(HDFGroup, ElementType);

    HDFGroup["value"].attributes.create_from<std::string>("units", Unit);

  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger::Error("f144 writer could not init_hdf hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the writer module interface, forward to the OPEN case of
/// `init_hdf`.
InitResult f144_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    Timestamp = NeXusDataset::Time(HDFGroup, Open);
    CueIndex = NeXusDataset::CueIndex(HDFGroup, Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, Open);
    Values = NeXusDataset::ExtensibleDatasetBase(HDFGroup, "value", Open);
  } catch (std::exception &E) {
    Logger::Error(
        R"(Failed to reopen datasets in HDF file with error message: "{}")",
        std::string(E.what()));
    return InitResult::ERROR;
  }
  return InitResult::OK;
}

template <typename FBValueType, typename ReturnType>
ReturnType extractScalarValue(const f144_LogData *LogDataMessage) {
  auto LogValue = LogDataMessage->value_as<FBValueType>();
  return LogValue->value();
}

template <typename DataType, typename ValueType, class DatasetType>
ValuesInformation appendScalarData(DatasetType &Dataset,
                                   const f144_LogData *LogDataMessage) {
  auto ScalarValue = extractScalarValue<ValueType, DataType>(LogDataMessage);
  Dataset.template appendElement<DataType>(ScalarValue);
  return {double(ScalarValue), double(ScalarValue), double(ScalarValue), 1};
}

void msgTypeIsConfigType(f144_Writer::Type ConfigType, Value MsgType) {
  std::unordered_map<Value, f144_Writer::Type> TypeComparison{
      {Value::ArrayByte, f144_Writer::Type::int8},
      {Value::Byte, f144_Writer::Type::int8},
      {Value::ArrayUByte, f144_Writer::Type::uint8},
      {Value::ArrayByte, f144_Writer::Type::uint8},
      {Value::ArrayShort, f144_Writer::Type::int16},
      {Value::Short, f144_Writer::Type::int16},
      {Value::ArrayUShort, f144_Writer::Type::uint16},
      {Value::UShort, f144_Writer::Type::uint16},
      {Value::ArrayInt, f144_Writer::Type::int32},
      {Value::Int, f144_Writer::Type::int32},
      {Value::ArrayUInt, f144_Writer::Type::uint32},
      {Value::UInt, f144_Writer::Type::uint32},
      {Value::ArrayLong, f144_Writer::Type::int64},
      {Value::Long, f144_Writer::Type::int64},
      {Value::ArrayULong, f144_Writer::Type::uint64},
      {Value::ULong, f144_Writer::Type::uint64},
      {Value::ArrayFloat, f144_Writer::Type::float32},
      {Value::Float, f144_Writer::Type::float32},
      {Value::ArrayDouble, f144_Writer::Type::float64},
      {Value::Double, f144_Writer::Type::float64},
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
  std::unordered_map<f144_Writer::Type, std::string> ConfigTypeString{
      {f144_Writer::Type::int8, "int8"},
      {f144_Writer::Type::uint8, "uint8"},
      {f144_Writer::Type::int16, "int16"},
      {f144_Writer::Type::uint16, "uint16"},
      {f144_Writer::Type::int32, "int32"},
      {f144_Writer::Type::uint32, "uint32"},
      {f144_Writer::Type::int64, "int64"},
      {f144_Writer::Type::uint64, "uint64"},
      {f144_Writer::Type::float32, "float32"},
      {f144_Writer::Type::float64, "float64"},
  };
  try {
    if (TypeComparison.at(MsgType) != ConfigType) {
      Logger::Info(
          "Configured data type ({}) is not the same as the f144 message "
          "type ({}).",
          ConfigTypeString.at(ConfigType), MsgTypeString.at(MsgType));
    }
  } catch (std::out_of_range const &) {
    Logger::Error("Got out of range error when comparing types.");
  }
}

bool f144_Writer::writeImpl(FlatbufferMessage const &Message,
                            [[maybe_unused]] bool is_buffered_message) {
  auto LogDataMessage = Getf144_LogData(Message.data());
  Timestamp.appendElement(LogDataMessage->timestamp());
  auto Type = LogDataMessage->value_type();

  if (!HasCheckedMessageType) {
    msgTypeIsConfigType(ElementType, Type);
    HasCheckedMessageType = true;
  }

  ValuesInformation CValuesInfo;
  switch (Type) {
  case Value::Byte:
    CValuesInfo =
        appendScalarData<const std::int8_t, Byte>(Values, LogDataMessage);
    break;
  case Value::UByte:
    CValuesInfo =
        appendScalarData<const std::uint8_t, UByte>(Values, LogDataMessage);
    break;
  case Value::Short:
    CValuesInfo =
        appendScalarData<const std::int16_t, Short>(Values, LogDataMessage);
    break;
  case Value::UShort:
    CValuesInfo =
        appendScalarData<const std::uint16_t, UShort>(Values, LogDataMessage);
    break;
  case Value::Int:
    CValuesInfo =
        appendScalarData<const std::int32_t, Int>(Values, LogDataMessage);
    break;
  case Value::UInt:
    CValuesInfo =
        appendScalarData<const std::uint32_t, UInt>(Values, LogDataMessage);
    break;
  case Value::Long:
    CValuesInfo =
        appendScalarData<const std::int64_t, Long>(Values, LogDataMessage);
    break;
  case Value::ULong:
    CValuesInfo =
        appendScalarData<const std::uint64_t, ULong>(Values, LogDataMessage);
    break;
  case Value::Float:
    CValuesInfo = appendScalarData<const float, Float>(Values, LogDataMessage);
    break;
  case Value::Double:
    CValuesInfo =
        appendScalarData<const double, Double>(Values, LogDataMessage);
    break;
  default:
    Logger::Info(
        "f144 writer module no longer supports array types, so ignoring "
        "message with source '{}'",
        LogDataMessage->source_name()->str());
    throw WriterModule::WriterException(
        "Unknown data type in f144 flatbuffer.");
  }

  ++NrOfWrites;
  if ((NrOfWrites - LastIndexAtWrite) / ValueIndexInterval.get_value() > 0) {
    LastIndexAtWrite = NrOfWrites;
    CueIndex.appendElement(NrOfWrites - 1);
    CueTimestampZero.appendElement(LogDataMessage->timestamp());
  }
  if (MetaData.get_value()) {
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
  return true;
}

void f144_Writer::register_meta_data(hdf5::node::Group const &HDFGroup,
                                     const MetaData::TrackerPtr &Tracker) {
  if (MetaData.get_value()) {
    MetaDataMin = MetaData::Value<double>(
        HDFGroup, "minimum_value", MetaData::basicDatasetWriter<double>,
        MetaData::basicAttributeWriter<std::string>);
    MetaDataMin.setAttribute("units", Unit.get_value());
    Tracker->registerMetaData(MetaDataMin);

    MetaDataMax = MetaData::Value<double>(
        HDFGroup, "maximum_value", MetaData::basicDatasetWriter<double>,
        MetaData::basicAttributeWriter<std::string>);
    MetaDataMax.setAttribute("units", Unit.get_value());
    Tracker->registerMetaData(MetaDataMax);

    MetaDataMean = MetaData::Value<double>(
        HDFGroup, "average_value", MetaData::basicDatasetWriter<double>,
        MetaData::basicAttributeWriter<std::string>);
    MetaDataMean.setAttribute("units", Unit.get_value());
    Tracker->registerMetaData(MetaDataMean);
  }
}

/// Register the writer module.
static WriterModule::Registry::Registrar<f144_Writer> RegisterWriter("f144",
                                                                     "f144");

} // namespace WriterModule::f144
