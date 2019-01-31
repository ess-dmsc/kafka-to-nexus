#include "HDFFile.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <array>
#include <chrono>
#include <ctime>
#include <date/date.h>
#include <date/tz.h>
#include <deque>
#include <flatbuffers/flatbuffers.h>
#include <hdf5.h>
#include <stack>
#include <unistd.h>

namespace FileWriter {

using nlohmann::json;
using std::string;
using std::vector;
using json_out_of_range = nlohmann::detail::out_of_range;

/// As a safeguard, limit the maximum dimensions of multi dimensional arrays
/// that we are willing to write
static size_t const MAX_DIMENSIONS_OF_ARRAY = 10;

/// As a safeguard, limit the maximum size of a string that we are willing to
/// write
static size_t const MAX_ALLOWED_STRING_LENGTH = 4 * 1024 * 1024;

template <typename T>
static void writeAttribute(hdf5::node::Node const &Node,
                           const std::string &Name, T Value) {
  hdf5::property::AttributeCreationList acpl;
  acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);
  Node.attributes.create<T>(Name, acpl).write(Value);
}

template <typename T>
static void writeAttribute(hdf5::node::Node const &Node,
                           const std::string &Name, std::vector<T> Values) {
  hdf5::property::AttributeCreationList acpl;
  acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);
  Node.attributes.create<T>(Name, {Values.size()}, acpl).write(Values);
}

template <typename DT>
static void appendValue(nlohmann::json const &Value, std::vector<DT> &Buffer) {
  if (Value.is_number_integer()) {
    Buffer.push_back(Value.get<int64_t>());
  } else if (Value.is_number_unsigned()) {
    Buffer.push_back(Value.get<uint64_t>());
  } else if (Value.is_number_float()) {
    Buffer.push_back(Value.get<double>());
  } else {
    auto What = fmt::format("Expect a numeric value but got: {}",
                            Value.dump().substr(0, 256));
    std::throw_with_nested(std::runtime_error(What));
  }
}

class StackItem {
public:
  explicit StackItem(nlohmann::json const &Value)
      : Value(Value), Size(Value.size()) {}
  void inc() { ++Index; }
  nlohmann::json const &value() { return Value.at(Index); }
  bool exhausted() const { return !(Index < Size); }

private:
  nlohmann::json const &Value;
  size_t Index = 0;
  size_t Size = 0;
};

template <typename _DataType> class NumericItemHandler {
public:
  using DataType = _DataType;
  static void append(std::vector<DataType> &Buffer, nlohmann::json const &Value,
                     size_t const) {
    appendValue(Value.get<DataType>(), Buffer);
  }
};

class StringItemHandler {
public:
  using DataType = std::string;
  static void append(std::vector<DataType> &Buffer, nlohmann::json const &Value,
                     size_t const) {
    Buffer.push_back(Value);
  }
};

class FixedStringItemHandler {
public:
  using DataType = char;
  static void append(std::vector<DataType> &Buffer, nlohmann::json const &Value,
                     size_t const ItemLength = 0) {
    if (ItemLength >= MAX_ALLOWED_STRING_LENGTH) {
      std::throw_with_nested(std::runtime_error(fmt::format(
          "Failed to allocate fixed-size string dataset, bad element size: {}",
          ItemLength)));
    }
    std::string String = Value;
    String.resize(ItemLength, '\0');
    std::copy_n(String.data(), String.size(), std::back_inserter(Buffer));
  }
};

template <typename DataHandler>
static std::vector<typename DataHandler::DataType>
populateBlob(nlohmann::json const &Value, size_t const GoalSize,
             size_t const ItemLength = 0) {
  using DataType = typename DataHandler::DataType;
  std::vector<DataType> Buffer;
  if (Value.is_array()) {
    std::stack<StackItem> Stack;
    Stack.emplace(Value);
    while (!Stack.empty()) {
      if (Stack.size() > MAX_DIMENSIONS_OF_ARRAY) {
        break;
      }
      if (Stack.top().exhausted()) {
        Stack.pop();
        continue;
      }
      auto const &Value = Stack.top().value();
      if (Value.is_array()) {
        Stack.top().inc();
        Stack.emplace(Value);
      } else {
        Stack.top().inc();
        DataHandler::append(Buffer, Value, ItemLength);
      }
    }
  } else {
    DataHandler::append(Buffer, Value, ItemLength);
  }
  if (GoalSize != 0 && Buffer.size() != GoalSize) {
    auto What =
        fmt::format("Failed to populate numeric blob, size mismatch: {} != {}",
                    Buffer.size(), GoalSize);
    std::throw_with_nested(std::runtime_error(What));
  }
  return Buffer;
}

template <typename T>
static void writeAttrNumeric(hdf5::node::Node const &Node,
                             std::string const &Name,
                             nlohmann::json const &Value) {
  size_t Length = 1;
  if (Value.is_array()) {
    Length = Value.size();
  }
  try {
    auto ValueData = populateBlob<NumericItemHandler<T>>(Value, Length);
    try {
      if (Value.is_array()) {
        writeAttribute(Node, Name, ValueData);
      } else {
        writeAttribute(Node, Name, ValueData[0]);
      }
    } catch (std::exception const &E) {
      std::throw_with_nested(std::runtime_error(
          fmt::format("Failed write for numeric attribute {} in {}: {}", Name,
                      std::string(Node.link().path()), E.what())));
    }
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Can not populate blob for attribute {} in {}: {}", Name,
                    std::string(Node.link().path()), E.what())));
  }
}

HDFFile::HDFFile() {
// Keep this.  Will be used later to test against different lib versions
#if H5_VERSION_GE(1, 8, 0) && H5_VERSION_LE(1, 10, 99)
  unsigned int maj, min, rel;
  H5get_libversion(&maj, &min, &rel);
#else
  static_assert(false, "Unexpected HDF version");
#endif
}

HDFFile::~HDFFile() {
  try {
    finalize();
    close();
  }
  // we do this to prevent destructor from throwing
  catch (std::exception &e) {
    LOG(spdlog::level::err, "HDFFile failed to close, stack:\n{}",
        hdf5::error::print_nested(e));
  }
}

void HDFFile::writeStringAttribute(hdf5::node::Node const &Node,
                                   const std::string &Name,
                                   const std::string &Value) {
  auto string_type = hdf5::datatype::String::variable();
  string_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  hdf5::property::AttributeCreationList AttributeCreationPropertyList;
  AttributeCreationPropertyList.character_encoding(
      hdf5::datatype::CharacterEncoding::UTF8);

  auto at = Node.attributes.create(Name, string_type, hdf5::dataspace::Scalar(),
                                   AttributeCreationPropertyList);
  at.write(Value, string_type);
}

template <typename T>
static void writeHDFISO8601Attribute(hdf5::node::Node const &Node,
                                     const std::string &Name, T &TimeStamp) {
  auto s2 = format("%Y-%m-%dT%H:%M:%S%z", TimeStamp);
  HDFFile::writeStringAttribute(Node, Name, s2);
}

void HDFFile::writeHDFISO8601AttributeCurrentTime(hdf5::node::Node const &Node,
                                                  std::string const &Name) {
  const date::time_zone *CurrentTimeZone;
  try {
    CurrentTimeZone = date::current_zone();
  } catch (const std::runtime_error &e) {
    LOG(spdlog::level::warn, "Failed to detect time zone for use in ISO8601 "
                      "timestamp in HDF file")
    CurrentTimeZone = date::locate_zone("UTC");
  }
  auto now = date::make_zoned(
      CurrentTimeZone,
      date::floor<std::chrono::milliseconds>(std::chrono::system_clock::now()));
  writeHDFISO8601Attribute(Node, Name, now);
}

void HDFFile::writeAttributes(hdf5::node::Node const &Node,
                              nlohmann::json const *Value) {
  if (Value == nullptr) {
    return;
  }
  if (Value->is_array()) {
    writeArrayOfAttributes(Node, *Value);
  } else if (Value->is_object()) {
    writeObjectOfAttributes(Node, *Value);
  }
}

/// \brief Write attributes defined in an array of attribute objects.
///
/// Unlike a single attribute object this allows specifying type and dataset.
///
/// \param Node         Nodeto write attributes on.
/// \param JsonValue    json value array of attribute objects.
void HDFFile::writeArrayOfAttributes(hdf5::node::Node const &Node,
                                     nlohmann::json const &Values) {
  if (!Values.is_array()) {
    return;
  }
  for (auto const &Attribute : Values) {
    if (Attribute.is_object()) {
      string Name;
      if (auto NameMaybe = find<std::string>("name", Attribute)) {
        Name = NameMaybe.inner();
      } else {
        continue;
      }
      if (auto const &ValuesMaybe = find<json>("values", Attribute)) {
        std::string DType;
        auto const &Values = ValuesMaybe.inner();
        uint32_t StringSize = 0;
        if (auto StringSizeMaybe = find<uint32_t>("string_size", Attribute)) {
          StringSize = StringSizeMaybe.inner();
        }
        auto Encoding = hdf5::datatype::CharacterEncoding::UTF8;
        if (auto EncodingString = find<std::string>("encoding", Attribute)) {
          if (EncodingString.inner() == "ascii") {
            Encoding = hdf5::datatype::CharacterEncoding::ASCII;
          }
        }
        if (auto AttrType = find<std::string>("type", Attribute)) {
          DType = AttrType.inner();
          writeAttrOfSpecifiedType(DType, Node, Name, StringSize, Encoding,
                                   Values);
        } else {
          if (Values.is_array()) {
            LOG(spdlog::level::warn, "Attributes with array values must specify type")
            continue;
          }
          writeScalarAttribute(Node, Name, Values);
        }
      }
    }
  }
}

void writeAttrStringVariableLength(hdf5::node::Node const &Node,
                                   std::string const &Name, json const &Values,
                                   hdf5::datatype::CharacterEncoding Encoding) {
  auto Type = hdf5::datatype::String::variable();
  Type.encoding(Encoding);
  Type.padding(hdf5::datatype::StringPad::NULLTERM);
  if (Values.is_array()) {
    auto ValueArray = populateBlob<StringItemHandler>(Values, Values.size());
    auto StringAttr = Node.attributes.create(
        Name, Type, hdf5::dataspace::Simple{{Values.size()}});
    StringAttr.write(ValueArray);
  } else {
    std::string const StringValue = Values.get<std::string>();
    auto StringAttr =
        Node.attributes.create(Name, Type, hdf5::dataspace::Scalar());
    StringAttr.write(StringValue, Type);
  }
}

void writeAttrStringFixedLength(hdf5::node::Node const &Node,
                                std::string const &Name, json const &Values,
                                size_t StringSize,
                                hdf5::datatype::CharacterEncoding Encoding) {
  hdf5::dataspace::Dataspace SpaceMem;
  if (Values.is_array()) {
    SpaceMem = hdf5::dataspace::Simple({Values.size()});
  } else {
    SpaceMem = hdf5::dataspace::Scalar();
  }
  try {
    auto Type = hdf5::datatype::String::fixed(StringSize);
    Type.encoding(Encoding);
    Type.padding(hdf5::datatype::StringPad::NULLTERM);
    auto Attribute = Node.attributes.create(Name, Type, SpaceMem);
    auto SpaceFile = Attribute.dataspace();
    try {
      auto S = hdf5::dataspace::Simple(SpaceFile);
      auto D = S.current_dimensions();
      LOG(spdlog::level::trace, "Simple {}  {}", D.size(), D.at(0));
    } catch (...) {
      try {
        auto S = hdf5::dataspace::Scalar(SpaceFile);
        LOG(spdlog::level::trace, "Scalar");
      } catch (...) {
        LOG(spdlog::level::err, "Unknown dataspace requested for fixed length "
                        "string dataset {}",
            Name);
      }
    }
    auto Data = populateBlob<FixedStringItemHandler>(Values, 0, StringSize);
    LOG(spdlog::level::trace, "StringSize: {}  Data.size(): {}", StringSize, Data.size());
    // Fixed string support seems broken in h5cpp
    if (0 > H5Awrite(static_cast<hid_t>(Attribute), static_cast<hid_t>(Type),
                     Data.data())) {
      throw std::runtime_error(fmt::format("Attribute {} write failed", Name));
    }
  } catch (std::exception const &) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Failed to write fixed-size string attribute {} in {}",
                    Name, static_cast<std::string>(Node.link().path()))));
  }
}

void writeAttrString(hdf5::node::Node const &Node, std::string const &Name,
                     nlohmann::json const &Values, size_t const StringSize,
                     hdf5::datatype::CharacterEncoding Encoding) {
  if (StringSize > 0) {
    writeAttrStringFixedLength(Node, Name, Values, StringSize, Encoding);
  } else {
    writeAttrStringVariableLength(Node, Name, Values, Encoding);
  }
}

/// \brief Write scalar or array attribute of specified type.
///
/// \param DType    type of the attribute values.
/// \param Node     group or dataset to add attribute to.
/// \param Name     name of the attribute.
/// \param Values   the attribute values.
void HDFFile::writeAttrOfSpecifiedType(
    std::string const &DType, hdf5::node::Node const &Node,
    std::string const &Name, uint32_t StringSize,
    hdf5::datatype::CharacterEncoding Encoding, nlohmann::json const &Values) {
  try {
    if (DType == "uint8") {
      writeAttrNumeric<uint8_t>(Node, Name, Values);
    }
    if (DType == "uint16") {
      writeAttrNumeric<uint16_t>(Node, Name, Values);
    }
    if (DType == "uint32") {
      writeAttrNumeric<uint32_t>(Node, Name, Values);
    }
    if (DType == "uint64") {
      writeAttrNumeric<uint64_t>(Node, Name, Values);
    }
    if (DType == "int8") {
      writeAttrNumeric<int8_t>(Node, Name, Values);
    }
    if (DType == "int16") {
      writeAttrNumeric<int16_t>(Node, Name, Values);
    }
    if (DType == "int32") {
      writeAttrNumeric<int32_t>(Node, Name, Values);
    }
    if (DType == "int64") {
      writeAttrNumeric<int64_t>(Node, Name, Values);
    }
    if (DType == "float") {
      writeAttrNumeric<float>(Node, Name, Values);
    }
    if (DType == "double") {
      writeAttrNumeric<double>(Node, Name, Values);
    }
    if (DType == "string") {
      writeAttrString(Node, Name, Values, StringSize, Encoding);
    }
  } catch (std::exception const &) {
    std::stringstream ss;
    ss << "Failed attribute write in ";
    ss << Node.link().path() << "/" << Name;
    ss << " type='" << DType << "'";
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

/// \brief Write attributes defined in an object of name-value pairs.
///
/// \param node   Node to write attributes on.
/// \param jsv    Json value object of attributes.
void HDFFile::writeObjectOfAttributes(hdf5::node::Node const &Node,
                                      nlohmann::json const &Values) {
  for (auto It = Values.begin(); It != Values.end(); ++It) {

    auto const Name = It.key();
    writeScalarAttribute(Node, Name, It.value());
  }
}

/// \brief Write a scalar attribute when the type is to be inferred.
///
/// \param Node         Group or dataset to write attribute to
/// \param Name         Name of the attribute
/// \param AttrValue    Json value containing the attribute value
void HDFFile::writeScalarAttribute(hdf5::node::Node const &Node,
                                   std::string const &Name,
                                   nlohmann::json const &Values) {
  if (Values.is_string()) {
    writeStringAttribute(Node, Name, Values);
  } else if (Values.is_number_integer()) {
    writeAttribute(Node, Name, Values.get<int64_t>());
  } else if (Values.is_number_unsigned()) {
    writeAttribute(Node, Name, Values.get<uint64_t>());
  } else if (Values.is_number_float()) {
    writeAttribute(Node, Name, Values.get<double>());
  }
}

void HDFFile::writeAttributesIfPresent(hdf5::node::Node const &Node,
                                       nlohmann::json const &Values) {
  if (auto AttributesMaybe = find<json>("attributes", Values)) {
    auto const Attributes = AttributesMaybe.inner();
    writeAttributes(Node, &Attributes);
  }
}

template <typename DT>
static void writeNumericDataset(
    hdf5::node::Group const &Node, const std::string &Name,
    hdf5::property::DatasetCreationList const &DatasetCreationPropertyList,
    hdf5::dataspace::Dataspace const &Dataspace, const nlohmann::json *Values) {

  try {
    auto Dataset = Node.create_dataset(Name, hdf5::datatype::create<DT>(),
                                       Dataspace, DatasetCreationPropertyList);
    try {
      auto Blob =
          populateBlob<NumericItemHandler<DT>>(*Values, Dataspace.size());
      try {
        Dataset.write(Blob);
      } catch (std::exception const &E) {
        std::throw_with_nested(std::runtime_error(
            fmt::format("Failed write for numeric attribute {} in {}: {}", Name,
                        std::string(Node.link().path()), E.what())));
      }
    } catch (std::exception const &E) {
      std::throw_with_nested(std::runtime_error(
          fmt::format("Failed populate_blob for numeric attribute {} in {}: {}",
                      Name, std::string(Node.link().path()), E.what())));
    }
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Failed write for numeric attribute {} in {}: {}", Name,
                    std::string(Node.link().path()), E.what())));
  }
}

void HDFFile::writeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, nlohmann::json const &Values) {

  try {
    auto DataType = hdf5::datatype::String::variable();
    DataType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    DataType.padding(hdf5::datatype::StringPad::NULLTERM);

    auto Dataset =
        Parent.create_dataset(Name, DataType, Dataspace, DatasetCreationList);
    Dataset.write(populateBlob<StringItemHandler>(Values, Dataspace.size()),
                  DataType, Dataspace, Dataspace,
                  hdf5::property::DatasetTransferList());
  } catch (const std::exception &e) {
    std::stringstream ss;
    ss << "Failed to write variable-size string dataset ";
    ss << Parent.link().path() << "/" << Name;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

void HDFFile::writeFixedSizeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, hsize_t ElementSize,
    const nlohmann::json *Values) {
  try {
    auto DataType = hdf5::datatype::String::fixed(ElementSize);
    DataType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    DataType.padding(hdf5::datatype::StringPad::NULLTERM);

    try {
      auto Space = hdf5::dataspace::Simple(Dataspace);
      auto Dimensions = Space.current_dimensions();
      LOG(spdlog::level::trace, "Simple {}  {}", Dimensions.size(), Dimensions.at(0));
    } catch (...) {
      try {
        auto Space = hdf5::dataspace::Scalar(Dataspace);
        LOG(spdlog::level::trace, "Scalar");
      } catch (...) {
        LOG(spdlog::level::err,
            "Unknown dataspace requested for fixed length string dataset {}",
            Name);
      }
    }

    auto Dataset =
        Parent.create_dataset(Name, DataType, Dataspace, DatasetCreationList);

    auto Data = populateBlob<FixedStringItemHandler>(*Values, 0, ElementSize);
    H5Dwrite(static_cast<hid_t>(Dataset), static_cast<hid_t>(DataType),
             static_cast<hid_t>(Dataspace), static_cast<hid_t>(Dataspace),
             H5P_DEFAULT, Data.data());
    /*
    Fixed string support seems broken in h5cpp.
    The analogue of the above should be:
    Dataset.write(Data.data(), DataType, Dataspace, Dataspace,
    hdf5::property::DatasetTransferList());
    which does not produce the expected result.
    */
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Failed to write fixed-size string dataset {} in {}", Name,
                    static_cast<std::string>(Parent.link().path()))));
  }
}

void HDFFile::writeGenericDataset(const std::string &DataType,
                                  hdf5::node::Group const &Parent,
                                  const std::string &Name,
                                  const std::vector<hsize_t> &Sizes,
                                  const std::vector<hsize_t> &Max,
                                  hsize_t ElementSize,
                                  const nlohmann::json *Values) {
  try {

    hdf5::property::DatasetCreationList DatasetCreationList;
    hdf5::dataspace::Dataspace Dataspace = hdf5::dataspace::Scalar();
    if (!Sizes.empty()) {
      Dataspace = hdf5::dataspace::Simple(Sizes, Max);
      if (Max[0] == H5S_UNLIMITED) {
        DatasetCreationList.chunk(Sizes);
      }
    }

    if (DataType == "uint8") {
      writeNumericDataset<uint8_t>(Parent, Name, DatasetCreationList, Dataspace,
                                   Values);
    }
    if (DataType == "uint16") {
      writeNumericDataset<uint16_t>(Parent, Name, DatasetCreationList,
                                    Dataspace, Values);
    }
    if (DataType == "uint32") {
      writeNumericDataset<uint32_t>(Parent, Name, DatasetCreationList,
                                    Dataspace, Values);
    }
    if (DataType == "uint64") {
      writeNumericDataset<uint64_t>(Parent, Name, DatasetCreationList,
                                    Dataspace, Values);
    }
    if (DataType == "int8") {
      writeNumericDataset<int8_t>(Parent, Name, DatasetCreationList, Dataspace,
                                  Values);
    }
    if (DataType == "int16") {
      writeNumericDataset<int16_t>(Parent, Name, DatasetCreationList, Dataspace,
                                   Values);
    }
    if (DataType == "int32") {
      writeNumericDataset<int32_t>(Parent, Name, DatasetCreationList, Dataspace,
                                   Values);
    }
    if (DataType == "int64") {
      writeNumericDataset<int64_t>(Parent, Name, DatasetCreationList, Dataspace,
                                   Values);
    }
    if (DataType == "float") {
      writeNumericDataset<float>(Parent, Name, DatasetCreationList, Dataspace,
                                 Values);
    }
    if (DataType == "double") {
      writeNumericDataset<double>(Parent, Name, DatasetCreationList, Dataspace,
                                  Values);
    }
    if (DataType == "string") {
      if (ElementSize == H5T_VARIABLE) {
        writeStringDataset(Parent, Name, DatasetCreationList, Dataspace,
                           *Values);
      } else {
        writeFixedSizeStringDataset(Parent, Name, DatasetCreationList,
                                    Dataspace, ElementSize, Values);
      }
    }
  } catch (std::exception const &) {
    std::stringstream ss;
    ss << "Failed dataset write in ";
    ss << Parent.link().path() << "/" << Name;
    ss << " type='" << DataType << "'";
    ss << " size(";
    for (auto &s : Sizes) {
      ss << s << " ";
    }
    ss << ")  max(";
    for (auto &s : Max) {
      ss << s << " ";
    }
    ss << ")  ";
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

void HDFFile::writeDataset(hdf5::node::Group const &Parent,
                           const nlohmann::json *Values) {
  std::string Name;
  if (auto NameMaybe = find<std::string>("name", *Values)) {
    Name = NameMaybe.inner();
  } else {
    return;
  }

  std::string DataType = "int64";
  hsize_t ElementSize = H5T_VARIABLE;

  std::vector<hsize_t> Sizes;
  if (auto DatasetJSONObject = find<json>("dataset", *Values)) {
    auto DatasetInnerObject = DatasetJSONObject.inner();
    if (auto DataSpaceObject = find<std::string>("space", DatasetInnerObject)) {
      if (DataSpaceObject.inner() != "simple") {
        LOG(spdlog::level::warn, "sorry, can only handle simple data spaces");
        return;
      }
    }

    if (auto DatasetTypeObject =
            find<std::string>("type", DatasetInnerObject)) {
      DataType = DatasetTypeObject.inner();
    }

    // optional, default to scalar
    if (auto DatasetSizeObject = find<json>("size", DatasetInnerObject)) {
      if (DatasetSizeObject.inner().is_array()) {
        auto DatasetSizeInnerObject = DatasetSizeObject.inner();
        for (auto const &Element : DatasetSizeInnerObject) {
          if (Element.is_number_integer()) {
            Sizes.push_back(Element.get<int64_t>());
          } else if (Element.is_string()) {
            if (Element.get<std::string>() == "unlimited") {
              Sizes.push_back(H5S_UNLIMITED);
            }
          }
        }
      }
    }

    if (auto DatasetStringSizeObject =
            find<uint64_t>("string_size", DatasetInnerObject)) {
      if ((DatasetStringSizeObject.inner() > 0) &&
          (DatasetStringSizeObject.inner() != H5T_VARIABLE)) {
        ElementSize = DatasetStringSizeObject.inner();
      }
    }
  }

  auto DatasetValuesObject = find<json>("values", *Values);
  if (!DatasetValuesObject) {
    return;
  }
  auto DatasetValuesInnerObject = DatasetValuesObject.inner();

  if (DatasetValuesInnerObject.is_number_float()) {
    DataType = "double";
  }

  auto Max = Sizes;
  if (!Sizes.empty()) {
    if (Sizes[0] == H5S_UNLIMITED) {
      if (DatasetValuesInnerObject.is_array()) {
        Sizes[0] = DatasetValuesInnerObject.size();
      } else {
        Sizes[0] = 1;
      }
    }
  }

  writeGenericDataset(DataType, Parent, Name, Sizes, Max, ElementSize,
                      &DatasetValuesInnerObject);
  auto dset = hdf5::node::Dataset(Parent.nodes[Name]);

  writeAttributesIfPresent(dset, *Values);
}

void HDFFile::createHDFStructures(
    const nlohmann::json *Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<StreamHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path) {

  try {

    // The HDF object that we will maybe create at the current level.
    hdf5::node::Group hdf_this;
    if (auto TypeMaybe = find<std::string>("type", *Value)) {
      auto Type = TypeMaybe.inner();
      if (Type == "group") {
        if (auto NameMaybe = find<std::string>("name", *Value)) {
          auto Name = NameMaybe.inner();
          try {
            hdf_this = Parent.create_group(Name, LinkCreationPropertyList);
            Path.push_back(Name);
          } catch (...) {
            LOG(spdlog::level::critical, "failed to create group  Name: {}", Name);
          }
        }
      }
      if (Type == "stream") {
        string pathstr;
        for (auto &x : Path) {
          pathstr += "/" + x;
        }

        HDFStreamInfo.push_back(StreamHDFInfo{pathstr, Value->dump()});
      }
      if (Type == "dataset") {
        writeDataset(Parent, Value);
      }
    }

    // If the current level in the HDF can act as a parent, then continue the
    // recursion with the (optional) "children" array.
    if (hdf_this.is_valid()) {
      writeAttributesIfPresent(hdf_this, *Value);
      if (auto ChildrenMaybe = find<json>("children", *Value)) {
        auto Children = ChildrenMaybe.inner();
        if (Children.is_array()) {
          for (auto &Child : Children) {
            createHDFStructures(&Child, hdf_this, Level + 1,
                                LinkCreationPropertyList, FixedStringHDFType,
                                HDFStreamInfo, Path);
          }
        }
      }
      Path.pop_back();
    }
  } catch (const std::exception &e) {
    // Don't throw here as the file should continue writing
    LOG(spdlog::level::err, "Failed to create structure  parent={} level={}",
        std::string(Parent.link().path()), Level)
  }
}

/// Human readable version of the HDF5 headers that we compile against.
std::string HDFFile::H5VersionStringHeadersCompileTime() {
  return fmt::format("{}.{}.{}", H5_VERS_MAJOR, H5_VERS_MINOR, H5_VERS_RELEASE);
}

/// Human readable version of the HDF5 libraries that we run with.
std::string HDFFile::h5VersionStringLinked() {
  unsigned h5_vers_major, h5_vers_minor, h5_vers_release;
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  return fmt::format("{}.{}.{}", h5_vers_major, h5_vers_minor, h5_vers_release);
}

/// Compare the version of the HDF5 headers which the kafka-to-nexus was
/// compiled with against the version of the HDF5 libraries that the
/// kafka-to-nexus is linked against at runtime. Currently, a mismatch in the
/// release number is logged but does not cause panic.
void HDFFile::checkHDFVersion() {
  unsigned h5_vers_major, h5_vers_minor, h5_vers_release;
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  if (h5_vers_major != H5_VERS_MAJOR) {
    LOG(spdlog::level::err, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        H5VersionStringHeadersCompileTime(), h5VersionStringLinked());
    exit(1);
  }
  if (h5_vers_minor != H5_VERS_MINOR) {
    LOG(spdlog::level::err, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        H5VersionStringHeadersCompileTime(), h5VersionStringLinked());
    exit(1);
  }
  if (h5_vers_release != H5_VERS_RELEASE) {
    LOG(spdlog::level::err, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        H5VersionStringHeadersCompileTime(), h5VersionStringLinked());
  }
}

extern "C" char const GIT_COMMIT[];

void HDFFile::init(std::string const &Filename,
                   nlohmann::json const &NexusStructure, nlohmann::json const &,
                   std::vector<StreamHDFInfo> &StreamHDFInfo, bool UseHDFSWMR) {
  if (std::ifstream(Filename).good()) {
    // File exists already
    throw std::runtime_error(
        fmt::format("The file \"{}\" exists already.", Filename));
  }
  try {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    setCommonProps(fcpl, fapl);
    if (UseHDFSWMR) {
      H5File =
          hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE |
                                           hdf5::file::AccessFlags::SWMR_WRITE,
                             fcpl, fapl);
      SWMREnabled = true;
    } else {
      H5File = hdf5::file::create(Filename, hdf5::file::AccessFlags::EXCLUSIVE,
                                  fcpl, fapl);
    }
    this->Filename = Filename;
    init(NexusStructure, StreamHDFInfo);
  } catch (std::exception const &E) {
    LOG(spdlog::level::err,
        "ERROR could not create the HDF  path={}  file={}  trace:\n{}",
        boost::filesystem::current_path().string(), Filename,
        hdf5::error::print_nested(E));
    std::throw_with_nested(std::runtime_error("HDFFile failed to open!"));
  }
  this->NexusStructure = NexusStructure;
}

void HDFFile::init(const std::string &NexusStructure,
                   std::vector<StreamHDFInfo> &StreamHDFInfo) {
  auto Document = nlohmann::json::parse(NexusStructure);
  init(Document, StreamHDFInfo);
}

void HDFFile::init(const nlohmann::json &NexusStructure,
                   std::vector<StreamHDFInfo> &StreamHDFInfo) {

  try {
    checkHDFVersion();

    hdf5::property::AttributeCreationList acpl;
    acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    hdf5::property::LinkCreationList lcpl;
    lcpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    auto var_string = hdf5::datatype::String::variable();
    var_string.encoding(hdf5::datatype::CharacterEncoding::UTF8);

    RootGroup = H5File.root();

    std::deque<std::string> path;
    if (NexusStructure.is_object()) {
      auto value = &NexusStructure;
      if (auto ChildrenMaybe = find<json>("children", *value)) {
        auto Children = ChildrenMaybe.inner();
        if (Children.is_array()) {
          for (auto &Child : Children) {
            createHDFStructures(&Child, RootGroup, 0, lcpl, var_string,
                                StreamHDFInfo, path);
          }
        }
      }
    }

    writeStringAttribute(RootGroup, "HDF5_Version", h5VersionStringLinked());
    writeStringAttribute(RootGroup, "file_name",
                         H5File.id().file_name().string());
    writeStringAttribute(
        RootGroup, "creator",
        fmt::format("kafka-to-nexus commit {:.7}", GIT_COMMIT));
    writeHDFISO8601AttributeCurrentTime(RootGroup, "file_time");
    writeAttributesIfPresent(RootGroup, NexusStructure);
  } catch (std::exception const &E) {
    LOG(spdlog::level::critical, "Failed to initialize  file={}  trace:\n{}",
        H5File.id().file_name().string(), hdf5::error::print_nested(E));
    std::throw_with_nested(std::runtime_error("HDFFile failed to initialize!"));
  }
}

void HDFFile::close() {
  try {
    if (H5File.is_valid()) {
      LOG(spdlog::level::trace, "flushing");
      flush();
      LOG(spdlog::level::trace, "closing");
      H5File.close();
      LOG(spdlog::level::trace, "closed");
      // Make sure that h5file.is_valid() == false from now on:
      H5File = hdf5::file::File();
    } else {
      // This occurs in unit tests
      LOG(spdlog::level::err, "File is not valid, skipping flush and close.");
    }
  } catch (const std::exception &E) {
    auto Trace = hdf5::error::print_nested(E);
    LOG(spdlog::level::err, "ERROR could not close  file={}  trace:\n{}",
        H5File.id().file_name().string(), Trace);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "HDFFile failed to close.  Current Path: {}  Filename: {}  Trace:\n{}",
        boost::filesystem::current_path().string(),
        H5File.id().file_name().string(), Trace)));
  }
}

void HDFFile::reopen(std::string const &Filename) {
  try {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    setCommonProps(fcpl, fapl);
    auto FAFL = static_cast<hdf5::file::AccessFlagsBase>(
        hdf5::file::AccessFlags::READWRITE);
    if (SWMREnabled) {
      FAFL |= static_cast<hdf5::file::AccessFlagsBase>(
          hdf5::file::AccessFlags::SWMR_WRITE);
    }
    H5File = hdf5::file::open(Filename, FAFL, fapl);
  } catch (std::exception const &E) {
    auto Trace = hdf5::error::print_nested(E);
    LOG(spdlog::level::err,
        "ERROR could not reopen HDF file  path={}  file={}  trace:\n{}",
        boost::filesystem::current_path().string(), Filename, Trace);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "HDFFile failed to reopen.  Current Path: {}  Filename: {}  Trace:\n{}",
        boost::filesystem::current_path().string(), Filename, Trace)));
  }
}

void HDFFile::flush() {
  try {
    if (H5File.is_valid()) {
      H5File.flush(hdf5::file::Scope::GLOBAL);
    }
  } catch (const std::runtime_error &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("HDFFile failed to flush  what: {}", E.what())));
  } catch (...) {
    std::throw_with_nested(
        std::runtime_error("HDFFile failed to flush with unknown exception"));
  }
}

static void addLinks(hdf5::node::Group const &Group,
                     nlohmann::json const &Json) {
  if (!Json.is_object()) {
    throw std::runtime_error(fmt::format(
        "HDFFile addLinks: We expect a json object but got: {}", Json.dump()));
  }
  auto ChildrenIter = Json.find("children");
  if (ChildrenIter == Json.end()) {
    return;
  }
  auto &Children = *ChildrenIter;
  if (!Children.is_array()) {
    throw std::runtime_error("HDFFile addLinks: \"children\" must be an array");
  }
  for (auto const &Child : Children) {
    if (!Child.is_object()) {
      continue;
    }
    if (Child.find("type") == Child.end()) {
      continue;
    }
    if (Child.at("type") != "group") {
      continue;
    }
    if (Child.find("name") == Child.end()) {
      continue;
    }
    auto ChildGroup = Group.get_group(Child.at("name").get<std::string>());
    addLinks(ChildGroup, Child);
  }
  for (auto const &Child : Children) {
    if (!Child.is_object()) {
      continue;
    }
    if (Child.find("type") == Child.end()) {
      continue;
    }
    if (Child.at("type") != "link") {
      continue;
    }
    if (Child.find("name") == Child.end()) {
      continue;
    }
    if (Child.find("target") == Child.end()) {
      continue;
    }
    auto LinkName = Child.at("name").get<std::string>();
    auto Target = Child.at("target").get<std::string>();
    auto GroupBase = Group;
    auto TargetBase = Target;
    while (TargetBase.find("../") == 0) {
      TargetBase = TargetBase.substr(3);
      GroupBase = GroupBase.link().parent();
    }
    auto TargetID =
        H5Oopen(static_cast<hid_t>(GroupBase), TargetBase.c_str(), H5P_DEFAULT);
    if (TargetID < 0) {
      LOG(spdlog::level::warn,
          "Can not find target object for link target: {}  in group: {}",
          Target, std::string(Group.link().path()));
      continue;
    }
    if (0 > H5Olink(TargetID, static_cast<hid_t>(Group), LinkName.c_str(),
                    H5P_DEFAULT, H5P_DEFAULT)) {
      LOG(spdlog::level::warn,
          "can not create link name: {}  in group: {}  to target: {}", LinkName,
          std::string(Group.link().path()), Target);
      continue;
    }
  }
}

void HDFFile::finalize() {
  LOG(spdlog::level::trace, "HDFFile::finalize");
  if (Filename.empty()) {
    LOG(spdlog::level::trace, "HDFFile was never open, skip finalize.");
    return;
  }
  try {
    if (H5File.is_valid()) {
      close();
    }
    hdf5::property::FileCreationList FCPL;
    hdf5::property::FileAccessList FAPL;
    setCommonProps(FCPL, FAPL);
    auto FAFL = static_cast<hdf5::file::AccessFlagsBase>(
        hdf5::file::AccessFlags::READWRITE);
    H5File = hdf5::file::open(Filename, FAFL, FAPL);
    auto Group = H5File.root();
    addLinks(Group, NexusStructure);
  } catch (...) {
    std::throw_with_nested(
        std::runtime_error(fmt::format("Exception in HDFFile::finalize")));
  }
}

void HDFFile::SWMRFlush() {
  auto Now = CLOCK::now();
  if (Now - SWMRFlushLast > SWMRFlushInterval) {
    flush();
    SWMRFlushLast = Now;
  }
}

bool HDFFile::isSWMREnabled() const { return SWMREnabled; }

} // namespace FileWriter
