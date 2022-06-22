// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "scal_Writer.h"
#include "MetaData/HDF5DataWriter.h"
#include "WriterRegistrar.h"
#include "json.h"
#include "logger.h"
#include <algorithm>
#include <cctype>
#include <scal_epics_scalar_data_generated.h>

namespace WriterModule::scal {

using nlohmann::json;

using Type = scal_Writer::Type;

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
void scal_Writer::config_post_processing() {
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
InitResult scal_Writer::init_hdf(hdf5::node::Group &HDFGroup) const {
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
    if (not Unit.getValue().empty()) {
      HDFGroup["value"].attributes.create_from<std::string>("units", Unit);
    }

  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    LOG_ERROR("scal writer could not init_hdf hdf_parent: {}  trace: {}",
              static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the writer module interface, forward to the OPEN case of
/// `init_hdf`.
InitResult scal_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    Timestamp = NeXusDataset::Time(HDFGroup, Open);
    CueIndex = NeXusDataset::CueIndex(HDFGroup, Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, Open);
    Values = NeXusDataset::MultiDimDatasetBase(HDFGroup, Open);
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
ReturnType extractScalarValue(const ScalarData *ScalarDataMessage) {
  auto ScalarValue = ScalarDataMessage->value_as<FBValueType>();
  return ScalarValue->value();
}

template <typename DataType, typename ValueType, class DatasetType>
ValuesInformation appendScalarData(DatasetType &Dataset,
                                   const ScalarData *ScalarDataMessage) {
  auto ScalarValue = extractScalarValue<ValueType, DataType>(ScalarDataMessage);
  Dataset.appendArray(hdf5::ArrayAdapter<const DataType>(&ScalarValue, 1), {1});
  return {double(ScalarValue), double(ScalarValue), double(ScalarValue), 1};
}

void msgTypeIsConfigType(scal_Writer::Type ConfigType, Value MsgType) {
  std::unordered_map<Value, scal_Writer::Type> TypeComparison{
    {Value::ArrayInt8, scal_Writer::Type::int8},
    {Value::Int8, scal_Writer::Type::int8},
    {Value::ArrayUInt8, scal_Writer::Type::uint8},
    {Value::UInt8, scal_Writer::Type::uint8},
    {Value::ArrayInt16, scal_Writer::Type::int16},
    {Value::Int16, scal_Writer::Type::int16},
    {Value::ArrayUInt16, scal_Writer::Type::uint16},
    {Value::UInt16, scal_Writer::Type::uint16},
    {Value::ArrayInt32, scal_Writer::Type::int32},
    {Value::Int32, scal_Writer::Type::int32},
    {Value::ArrayUInt32, scal_Writer::Type::uint32},
    {Value::UInt32, scal_Writer::Type::uint32},
    {Value::ArrayInt64, scal_Writer::Type::int64},
    {Value::Int64, scal_Writer::Type::int64},
    {Value::ArrayUInt64, scal_Writer::Type::uint64},
    {Value::UInt64, scal_Writer::Type::uint64},
    {Value::ArrayFloat32, scal_Writer::Type::float32},
    {Value::Float32, scal_Writer::Type::float32},
    {Value::ArrayFloat64, scal_Writer::Type::float64},
    {Value::Float64, scal_Writer::Type::float64},
  };
  std::unordered_map<Value, std::string> MsgTypeString{
      {Value::ArrayInt8, "int8"},      {Value::Int8, "int8"},
      {Value::ArrayUInt8, "uint8"},    {Value::UInt8, "uint8"},
      {Value::ArrayInt16, "int16"},    {Value::Int16, "int16"},
      {Value::ArrayUInt16, "uint16"},  {Value::UInt16, "uint16"},
      {Value::ArrayInt32, "int32"},      {Value::Int32, "int32"},
      {Value::ArrayUInt32, "uint32"},    {Value::UInt32, "uint32"},
      {Value::ArrayInt64, "int64"},     {Value::UInt64, "int64"},
      {Value::ArrayUInt64, "uint64"},   {Value::UInt64, "uint64"},
      {Value::ArrayFloat32, "float32"},  {Value::Float32, "float32"},
      {Value::ArrayFloat64, "float64"}, {Value::Float64, "float64"},
  };
  std::unordered_map<scal_Writer::Type, std::string> ConfigTypeString{
      {scal_Writer::Type::int8, "int8"},
      {scal_Writer::Type::uint8, "uint8"},
      {scal_Writer::Type::int16, "int16"},
      {scal_Writer::Type::uint16, "uint16"},
      {scal_Writer::Type::int32, "int32"},
      {scal_Writer::Type::uint32, "uint32"},
      {scal_Writer::Type::int64, "int64"},
      {scal_Writer::Type::uint64, "uint64"},
      {scal_Writer::Type::float32, "float32"},
      {scal_Writer::Type::float64, "float64"},
  };
  try {
    if (TypeComparison.at(MsgType) != ConfigType) {
      LOG_WARN("Configured data type ({}) is not the same as the scal message "
               "type ({}).",
               ConfigTypeString.at(ConfigType), MsgTypeString.at(MsgType));
    }
  } catch (std::out_of_range const &) {
    LOG_ERROR("Got out of range error when comparing types.");
  }
}

void scal_Writer::write(FlatbufferMessage const &Message) {
  auto ScalarDataMessage = GetScalarData(Message.data());
  size_t NrOfElements{1};
  Timestamp.appendElement(ScalarDataMessage->timestamp());
  auto Type = ScalarDataMessage->value_type();

  if (not HasCheckedMessageType) {
    msgTypeIsConfigType(ElementType, Type);
    HasCheckedMessageType = true;
  }

  // Note that we are using our knowledge about flatbuffers here to minimise
  // amount of code we have to write by using some pointer arithmetic.
  auto DataPtr = reinterpret_cast<void const *>(
      reinterpret_cast<uint8_t const *>(ScalarDataMessage->value()) + 4);

  auto extractArrayInfo = [&NrOfElements, &DataPtr]() {
    NrOfElements = *(reinterpret_cast<int const *>(DataPtr) + 1);
    DataPtr = reinterpret_cast<void const *>(
        reinterpret_cast<int const *>(DataPtr) + 2);
  };

  ValuesInformation CValuesInfo;
  switch (Type) {
  case Value::ArrayInt8:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int8_t>(Values, DataPtr, NrOfElements,
                                                MetaData.getValue());
    break;
  case Value::Int8:
    CValuesInfo =
        appendScalarData<const std::int8_t, Int8>(Values, ScalarDataMessage);
    break;
  case Value::ArrayUInt8:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint8_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::UInt8:
    CValuesInfo =
        appendScalarData<const std::uint8_t, UInt8>(Values, ScalarDataMessage);
    break;
  case Value::ArrayInt16:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int16_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::Int16:
    CValuesInfo =
        appendScalarData<const std::int16_t, Int16>(Values, ScalarDataMessage);
    break;
  case Value::ArrayUInt16:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint16_t>(Values, DataPtr, NrOfElements,
                                                  MetaData.getValue());
    break;
  case Value::UInt16:
    CValuesInfo =
        appendScalarData<const std::uint16_t, Int16>(Values, ScalarDataMessage);
    break;
  case Value::ArrayInt32:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int32_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::Int32:
    CValuesInfo =
        appendScalarData<const std::int32_t, Int32>(Values, ScalarDataMessage);
    break;
  case Value::ArrayUInt32:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint32_t>(Values, DataPtr, NrOfElements,
                                                  MetaData.getValue());
    break;
  case Value::UInt32:
    CValuesInfo =
        appendScalarData<const std::uint32_t, UInt32>(Values, ScalarDataMessage);
    break;
  case Value::ArrayInt64:
    extractArrayInfo();
    CValuesInfo = appendData<const std::int64_t>(Values, DataPtr, NrOfElements,
                                                 MetaData.getValue());
    break;
  case Value::Int64:
    CValuesInfo =
        appendScalarData<const std::int64_t, Int64>(Values, ScalarDataMessage);
    break;
  case Value::ArrayUInt64:
    extractArrayInfo();
    CValuesInfo = appendData<const std::uint64_t>(Values, DataPtr, NrOfElements,
                                                  MetaData.getValue());
    break;
  case Value::UInt64:
    CValuesInfo =
        appendScalarData<const std::uint64_t, UInt64>(Values, ScalarDataMessage);
    break;
  case Value::ArrayFloat32:
    extractArrayInfo();
    CValuesInfo = appendData<const float>(Values, DataPtr, NrOfElements,
                                          MetaData.getValue());
    break;
  case Value::Float32:
    CValuesInfo = appendScalarData<const float, Float32>(Values, ScalarDataMessage);
    break;
  case Value::ArrayFloat64:
    extractArrayInfo();
    CValuesInfo = appendData<const double>(Values, DataPtr, NrOfElements,
                                           MetaData.getValue());
    break;
  case Value::Float64:
    CValuesInfo =
        appendScalarData<const double, Float64>(Values, ScalarDataMessage);
    break;
  default:
    throw WriterModule::WriterException(
        "Unknown data type in scal flatbuffer.");
  }

  ++NrOfWrites;
  if ((NrOfWrites - LastIndexAtWrite) / ValueIndexInterval.getValue() > 0) {
    LastIndexAtWrite = NrOfWrites;
    CueIndex.appendElement(NrOfWrites - 1);
    CueTimestampZero.appendElement(ScalarDataMessage->timestamp());
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
}

void scal_Writer::register_meta_data(hdf5::node::Group const &HDFGroup,
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
static WriterModule::Registry::Registrar<scal_Writer> RegisterWriter("scal",
                                                                     "scal");

} // namespace WriterModule::scal
