// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "f142_Writer.h"
#include "HDFFile.h"
#include "json.h"
#include <algorithm>
#include <cctype>
#include <f142_logdata_generated.h>
#include "WriterRegistrar.h"

namespace Module {
namespace f142 {

using nlohmann::json;

using Type = f142_Writer::Type;

std::string f142_Writer::findDataType(nlohmann::basic_json<> const &Attribute) {
  auto toLower = [](auto InString) {
    std::transform(InString.begin(), InString.end(), InString.begin(),
                   [](auto C) { return std::tolower(C); });
    return InString;
  };

  if (Attribute.find("type") != Attribute.end()) {
    return toLower(Attribute.find("type").value().get<std::string>());
  }
  if (Attribute.find("dtype") != Attribute.end()) {
    return toLower(Attribute.find("dtype").value().get<std::string>());
  }
  Logger->warn("Unable to find data type in JSON structure, using the default "
               "(double).");
  return "double";
}

template <typename Type>
void makeIt(hdf5::node::Group const &Parent, hdf5::Dimensions const &Shape,
            hdf5::Dimensions const &ChunkSize) {
  NeXusDataset::MultiDimDataset<Type>( // NOLINT(bugprone-unused-raii)
      Parent, NeXusDataset::Mode::Create, Shape,
      ChunkSize); // NOLINT(bugprone-unused-raii)
}

void initValueDataset(hdf5::node::Group &Parent, Type ElementType,
                      hdf5::Dimensions const &Shape,
                      hdf5::Dimensions const &ChunkSize,
                      nonstd::optional<std::string> const &ValueUnits) {
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

  if (ValueUnits) {
    Parent["value"].attributes.create_from<std::string>("units", *ValueUnits);
  }
}

/// Parse the configuration for this stream.
void f142_Writer::parse_config(std::string const &ConfigurationStream) {
  auto ConfigurationStreamJson = json::parse(ConfigurationStream);

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
    ElementType = TypeMap.at(findDataType(ConfigurationStreamJson));
  } catch (std::out_of_range &E) {
    Logger->warn("Unknown data type with name \"{}\". Using double.",
                 findDataType(ConfigurationStreamJson));
  }

  if (auto ArraySizeMaybe =
          find<uint64_t>("array_size", ConfigurationStreamJson)) {
    ArraySize = size_t(*ArraySizeMaybe);
  }

  ValueUnits = find<std::string>("value_units", ConfigurationStreamJson);

  try {
    ValueIndexInterval =
        ConfigurationStreamJson["nexus.cue_interval"].get<uint64_t>();
    Logger->trace("Value index interval: {}", ValueIndexInterval);
  } catch (...) { /* it's ok if not found */
  }
  try {
    ChunkSize = ConfigurationStreamJson["nexus.chunk_size"].get<uint64_t>();
    Logger->trace("Chunk size: {}", ChunkSize);
  } catch (...) { /* it's ok if not found */
  }
}

/// \brief Implement the HDFWriterModule interface, forward to the CREATE case
/// of
/// `init_hdf`.
InitResult f142_Writer::init_hdf(hdf5::node::Group &HDFGroup,
                                              std::string const &) {
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
                     {ChunkSize, ArraySize}, ValueUnits);

    if (HDFGroup.attributes.exists("NX_class")) {
      Logger->info("NX_class already specified!");
    } else {
      auto ClassAttribute = HDFGroup.attributes.create<std::string>("NX_class");
      ClassAttribute.write("NXlog");
    }
  } catch (std::exception const &E) {
    auto message = hdf5::error::print_nested(E);
    Logger->error("f142 could not init hdf_parent: {}  trace: {}",
                  static_cast<std::string>(HDFGroup.link().path()), message);
    return InitResult::ERROR;
  }

  return InitResult::OK;
}

/// \brief Implement the HDFWriterModule interface, forward to the OPEN case of
/// `init_hdf`.
InitResult f142_Writer::reopen(hdf5::node::Group &HDFGroup) {
  auto Open = NeXusDataset::Mode::Open;
  try {
    Timestamp = NeXusDataset::Time(HDFGroup, Open);
    CueIndex = NeXusDataset::CueIndex(HDFGroup, Open);
    CueTimestampZero = NeXusDataset::CueTimestampZero(HDFGroup, Open);
    Values = NeXusDataset::MultiDimDatasetBase(HDFGroup, Open);
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

void f142_Writer::write(FlatbufferMessage const &Message) {
  auto Flatbuffer = GetLogData(Message.data());
  size_t NrOfElements{1};
  Timestamp.appendElement(Flatbuffer->timestamp());
  auto Type = Flatbuffer->value_type();

  // Note that we are using our knowledge about flatbuffers here to minimise
  // amount of code we have to write by using some pointer arithmetric.
  auto DataPtr = reinterpret_cast<void const *>(
      reinterpret_cast<uint8_t const *>(Flatbuffer->value()) + 4);

  auto extractArrayInfo = [&NrOfElements, &DataPtr]() {
    NrOfElements = *(reinterpret_cast<int const *>(DataPtr) + 1);
    DataPtr = reinterpret_cast<void const *>(
        reinterpret_cast<int const *>(DataPtr) + 2);
  };
  switch (Type) {
  case Value::ArrayByte:
    extractArrayInfo(); // fallthrough
  case Value::Byte:
    appendData<const std::int8_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayUByte:
    extractArrayInfo(); // fallthrough
  case Value::UByte:
    appendData<const std::uint8_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayShort:
    extractArrayInfo(); // fallthrough
  case Value::Short:
    appendData<const std::int16_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayUShort:
    extractArrayInfo(); // fallthrough
  case Value::UShort:
    appendData<const std::uint16_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayInt:
    extractArrayInfo(); // fallthrough
  case Value::Int:
    appendData<const std::int32_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayUInt:
    extractArrayInfo(); // fallthrough
  case Value::UInt:
    appendData<const std::uint32_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayLong:
    extractArrayInfo(); // fallthrough
  case Value::Long:
    appendData<const std::int64_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayULong:
    extractArrayInfo(); // fallthrough
  case Value::ULong:
    appendData<const std::uint64_t>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayFloat:
    extractArrayInfo(); // fallthrough
  case Value::Float:
    appendData<const float>(Values, DataPtr, NrOfElements);
    break;
  case Value::ArrayDouble:
    extractArrayInfo(); // fallthrough
  case Value::Double:
    appendData<const double>(Values, DataPtr, NrOfElements);
    break;
  default:
    throw Module::WriterException(
        "Unknown data type in f142 flatbuffer.");
  }
}
/// Register the writer module.
static Module::Registry::Registrar<f142_Writer>
    RegisterWriter("f142", "general_epics_writer");

} // namespace f142
} // namespace Module
