// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFOperations.h"
#include "TimeUtility.h"
#include "json.h"
#include <stack>
#include <string>

namespace HDFOperations {
using nlohmann::json;
/// As a safeguard, limit the maximum dimensions of multi dimensional arrays
/// that we are willing to write
static size_t const MAX_DIMENSIONS_OF_ARRAY = 10;

/// As a safeguard, limit the maximum size of a string that we are willing to
/// write
static size_t const MAX_ALLOWED_STRING_LENGTH = 4 * 1024 * 1024;

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
    if (not Value.is_string()) {
      Buffer.push_back(Value.dump());
    } else {
      Buffer.push_back(Value);
    }
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

class StackItem {
public:
  explicit StackItem(nlohmann::json const &Value)
      : Value(Value), Size(Value.size()) {}
  void inc() { ++Index; }
  nlohmann::json const &value() { return Value.at(Index); }
  bool exhausted() const { return Index >= Size; }

private:
  nlohmann::json const &Value;
  size_t Index = 0;
  size_t Size = 0;
};

template <typename DataHandler>
static std::vector<typename DataHandler::DataType>
populateBlob(nlohmann::json const &ValueJson, size_t const GoalSize,
             size_t const ItemLength = 0) {
  using DataType = typename DataHandler::DataType;
  std::vector<DataType> Buffer;
  if (ValueJson.is_array()) {
    std::stack<StackItem> Stack;
    Stack.emplace(ValueJson);
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
    DataHandler::append(Buffer, ValueJson, ItemLength);
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

void writeStringAttribute(hdf5::node::Node const &Node, const std::string &Name,
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

void writeHDFISO8601AttributeCurrentTime(hdf5::node::Node const &Node,
                                         const std::string &Name) {
  writeStringAttribute(Node, Name, toLocalDateTime(system_clock::now()));
}

void writeAttributes(hdf5::node::Node const &Node, nlohmann::json const *Value,
                     SharedLogger const &Logger) {
  if (Value == nullptr) {
    return;
  }
  if (Value->is_array()) {
    writeArrayOfAttributes(Node, *Value, Logger);
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
/// \param Logger Pointer to spdlog instance to be used for logging.
void writeArrayOfAttributes(hdf5::node::Node const &Node,
                            const nlohmann::json &ValuesJson,
                            SharedLogger const &Logger) {
  if (!ValuesJson.is_array()) {
    return;
  }
  for (auto const &Attribute : ValuesJson) {
    if (Attribute.is_object()) {
      std::string Name;
      if (auto NameMaybe = find<std::string>("name", Attribute)) {
        Name = *NameMaybe;
      } else {
        continue;
      }
      if (auto const &ValuesMaybe = find<json>("values", Attribute)) {
        std::string DType{"double"};
        auto const &Values = *ValuesMaybe;
        uint32_t StringSize = 0;
        if (auto StringSizeMaybe = find<uint32_t>("string_size", Attribute)) {
          StringSize = *StringSizeMaybe;
        }
        using EncodingType = hdf5::datatype::CharacterEncoding;
        auto Encoding = EncodingType::UTF8;
        if (auto EncodingString = find<std::string>("encoding", Attribute)) {
          if (*EncodingString == "ascii") {
            Encoding = EncodingType::ASCII;
          }
        }
        if (Node.attributes.exists(Name)) {
          Node.attributes.remove(Name);
          LOG_DEBUG("Replacing (existing) attribute with key \"{}\".", Name);
        }
        if (Values.is_array() or StringSize > 0 or
            Encoding != EncodingType::UTF8) {
          if (findType(Attribute, DType)) {
            Logger->warn("No type defined for attribute, using the default.");
          }

          for (auto const &Elem : Values) {
            // cppcheck-suppress useStlAlgorithm
            if (Elem.is_string()) {
              DType = "string";
              break;
            }
          }
          writeAttrOfSpecifiedType(DType, Node, Name, StringSize, Encoding,
                                   Values, Logger);
        } else {
          writeScalarAttribute(Node, Name, Values);
        }
      }
    }
  }
}

bool findType(const nlohmann::basic_json<> Attribute, std::string &DType) {
  auto AttrType = find<std::string>("type", Attribute);
  if (AttrType) {
    DType = *AttrType;
    return true;
  } else {
    AttrType = find<std::string>("dtype", Attribute);
    if (AttrType) {
      DType = *AttrType;
      return true;
    } else
      return false;
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
                                hdf5::datatype::CharacterEncoding Encoding,
                                SharedLogger const &Logger) {
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
      Logger->trace("Simple {}  {}", D.size(), D.at(0));
    } catch (...) {
      try {
        auto S = hdf5::dataspace::Scalar(SpaceFile);
        Logger->trace("Scalar");
      } catch (...) {
        Logger->error("Unknown dataspace requested for fixed length "
                      "string dataset {}",
                      Name);
      }
    }
    auto Data = populateBlob<FixedStringItemHandler>(Values, 0, StringSize);
    Logger->trace("StringSize: {}  Data.size(): {}", StringSize, Data.size());
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
                     hdf5::datatype::CharacterEncoding Encoding,
                     SharedLogger const &Logger) {
  if (StringSize > 0) {
    writeAttrStringFixedLength(Node, Name, Values, StringSize, Encoding,
                               Logger);
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
/// \param Logger Pointer to spdlog instance to be used for logging.
void writeAttrOfSpecifiedType(std::string const &DType,
                              hdf5::node::Node const &Node,
                              std::string const &Name, uint32_t StringSize,
                              hdf5::datatype::CharacterEncoding Encoding,
                              nlohmann::json const &Values,
                              SharedLogger const &Logger) {
  try {
    std::map<std::string, std::function<void()>> WriteAttrMap{
        {"uint8", [&]() { writeAttrNumeric<uint8_t>(Node, Name, Values); }},
        {"uint16", [&]() { writeAttrNumeric<uint16_t>(Node, Name, Values); }},
        {"uint32", [&]() { writeAttrNumeric<uint32_t>(Node, Name, Values); }},
        {"uint64", [&]() { writeAttrNumeric<uint64_t>(Node, Name, Values); }},
        {"int8", [&]() { writeAttrNumeric<int8_t>(Node, Name, Values); }},
        {"int16", [&]() { writeAttrNumeric<int16_t>(Node, Name, Values); }},
        {"int32", [&]() { writeAttrNumeric<int32_t>(Node, Name, Values); }},
        {"int64", [&]() { writeAttrNumeric<int64_t>(Node, Name, Values); }},
        {"float", [&]() { writeAttrNumeric<float>(Node, Name, Values); }},
        {"double", [&]() { writeAttrNumeric<double>(Node, Name, Values); }},
        {"string", [&]() {
           writeAttrString(Node, Name, Values, StringSize, Encoding, Logger);
         }}};
    WriteAttrMap.at(DType)();
  } catch (std::exception const &) {
    auto ErrorStr =
        fmt::format("Failed attribute write in {}/{} with data type {}",
                    std::string(Node.link().path()), Name, DType);
    std::throw_with_nested(std::runtime_error(ErrorStr));
  }
}

/// \brief Write attributes defined in an object of name-value pairs.
///
/// \param node   Node to write attributes on.
/// \param jsv    Json value object of attributes.
void writeObjectOfAttributes(hdf5::node::Node const &Node,
                             nlohmann::json const &Values) {
  for (auto It = Values.cbegin(); It != Values.cend(); ++It) {
    auto const Name = It.key();
    if (Node.attributes.exists(Name)) {
      Node.attributes.remove(Name);
      LOG_DEBUG("Replacing (existing) attribute with key \"{}\".", Name);
    }
    writeScalarAttribute(Node, Name, It.value());
  }
}

/// \brief Write a scalar attribute when the type is to be inferred.
///
/// \param Node         Group or dataset to write attribute to
/// \param Name         Name of the attribute
/// \param AttrValue    Json value containing the attribute value
void writeScalarAttribute(hdf5::node::Node const &Node, std::string const &Name,
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

void writeAttributesIfPresent(hdf5::node::Node const &Node,
                              nlohmann::json const &Values,
                              SharedLogger const &Logger) {
  if (auto AttributesMaybe = find<json>("attributes", Values)) {
    auto const Attributes = *AttributesMaybe;
    writeAttributes(Node, &Attributes, Logger);
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

void writeStringDataset(
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
    auto ErrorStr =
        fmt::format("Failed to write variable-size string dataset {}/{}.",
                    std::string(Parent.link().path()), Name);
    std::throw_with_nested(std::runtime_error(ErrorStr));
  }
}

void writeFixedSizeStringDataset(
    hdf5::node::Group const &Parent, const std::string &Name,
    hdf5::property::DatasetCreationList &DatasetCreationList,
    hdf5::dataspace::Dataspace &Dataspace, hsize_t ElementSize,
    const nlohmann::json *Values, SharedLogger const &Logger) {
  try {
    auto DataType = hdf5::datatype::String::fixed(ElementSize);
    DataType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    DataType.padding(hdf5::datatype::StringPad::NULLTERM);

    try {
      auto Space = hdf5::dataspace::Simple(Dataspace);
      auto Dimensions = Space.current_dimensions();
      Logger->trace("Simple {}  {}", Dimensions.size(), Dimensions.at(0));
    } catch (...) {
      try {
        auto Space = hdf5::dataspace::Scalar(Dataspace);
        Logger->trace("Scalar");
      } catch (...) {
        Logger->error(
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

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name,
                         const std::vector<hsize_t> &Sizes,
                         const std::vector<hsize_t> &Max, hsize_t ElementSize,
                         const nlohmann::json *Values,
                         SharedLogger const &Logger) {
  try {

    hdf5::property::DatasetCreationList DatasetCreationList;
    hdf5::dataspace::Dataspace Dataspace = hdf5::dataspace::Scalar();
    if (!Sizes.empty()) {
      Dataspace = hdf5::dataspace::Simple(Sizes, Max);
      if (Max[0] == H5S_UNLIMITED) {
        DatasetCreationList.chunk(Sizes);
      }
    }
    std::map<std::string, std::function<void()>> WriteDatasetMap{
        {"uint8",
         [&]() {
           writeNumericDataset<uint8_t>(Parent, Name, DatasetCreationList,
                                        Dataspace, Values);
         }},
        {"uint16",
         [&]() {
           writeNumericDataset<uint16_t>(Parent, Name, DatasetCreationList,
                                         Dataspace, Values);
         }},
        {"uint32",
         [&]() {
           writeNumericDataset<uint32_t>(Parent, Name, DatasetCreationList,
                                         Dataspace, Values);
         }},
        {"uint64",
         [&]() {
           writeNumericDataset<uint64_t>(Parent, Name, DatasetCreationList,
                                         Dataspace, Values);
         }},
        {"int8",
         [&]() {
           writeNumericDataset<int8_t>(Parent, Name, DatasetCreationList,
                                       Dataspace, Values);
         }},
        {"int16",
         [&]() {
           writeNumericDataset<int16_t>(Parent, Name, DatasetCreationList,
                                        Dataspace, Values);
         }},
        {"int32",
         [&]() {
           writeNumericDataset<int32_t>(Parent, Name, DatasetCreationList,
                                        Dataspace, Values);
         }},
        {"int64",
         [&]() {
           writeNumericDataset<int64_t>(Parent, Name, DatasetCreationList,
                                        Dataspace, Values);
         }},
        {"float",
         [&]() {
           writeNumericDataset<float>(Parent, Name, DatasetCreationList,
                                      Dataspace, Values);
         }},
        {"double",
         [&]() {
           writeNumericDataset<double>(Parent, Name, DatasetCreationList,
                                       Dataspace, Values);
         }},
        {"string",
         [&]() {
           if (ElementSize == H5T_VARIABLE) {
             writeStringDataset(Parent, Name, DatasetCreationList, Dataspace,
                                *Values);
           } else {
             writeFixedSizeStringDataset(Parent, Name, DatasetCreationList,
                                         Dataspace, ElementSize, Values,
                                         Logger);
           }
         }},
    };
    WriteDatasetMap.at(DataType)();
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

void writeDataset(hdf5::node::Group const &Parent, const nlohmann::json *Values,
                  SharedLogger const &Logger) {
  std::string Name;
  if (auto NameMaybe = find<std::string>("name", *Values)) {
    Name = *NameMaybe;
  } else {
    return;
  }

  std::string DataType = "int64";
  hsize_t ElementSize = H5T_VARIABLE;

  std::vector<hsize_t> Sizes;
  if (auto DatasetJSONObject = find<json>("dataset", *Values)) {
    auto DatasetInnerObject = *DatasetJSONObject;
    if (auto DataSpaceObject = find<std::string>("space", DatasetInnerObject)) {
      if (*DataSpaceObject != "simple") {
        Logger->warn("sorry, can only handle simple data spaces");
        return;
      }
    }
    findType(DatasetInnerObject, DataType);
    // optional, default to scalar
    if (auto DatasetSizeObject = find<json>("size", DatasetInnerObject)) {
      auto DatasetSizeInnerObject = *DatasetSizeObject;
      if (DatasetSizeInnerObject.is_array()) {
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
      if ((*DatasetStringSizeObject > 0) &&
          (*DatasetStringSizeObject != H5T_VARIABLE)) {
        ElementSize = *DatasetStringSizeObject;
      }
    }
  }

  auto DatasetValuesObject = find<json>("values", *Values);
  if (!DatasetValuesObject) {
    return;
  }
  auto DatasetValuesInnerObject = *DatasetValuesObject;

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
                      &DatasetValuesInnerObject, Logger);
  auto dset = hdf5::node::Dataset(Parent.nodes[Name]);

  writeAttributesIfPresent(dset, *Values, Logger);
}

void createHDFStructures(
    const nlohmann::json *Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<StreamHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path,
    SharedLogger const &Logger) {

  try {

    // The HDF object that we will maybe create at the current level.
    hdf5::node::Group hdf_this;
    std::string Type;
    if (findType(*Value, Type)) {
      if (Type == "group") {
        if (auto NameMaybe = find<std::string>("name", *Value)) {
          auto Name = *NameMaybe;
          try {
            hdf_this = Parent.create_group(Name, LinkCreationPropertyList);
            Path.push_back(Name);
          } catch (...) {
            Logger->critical("failed to create group  Name: {}", Name);
          }
        }
      }
      if (Type == "stream") {
        std::string pathstr;
        for (auto &x : Path) {
          // cppcheck-suppress useStlAlgorithm
          pathstr += "/" + x;
        }

        HDFStreamInfo.push_back(StreamHDFInfo{pathstr, Value->dump()});
      }
      if (Type == "dataset") {
        writeDataset(Parent, Value, Logger);
      }
    }

    // If the current level in the HDF can act as a parent, then continue the
    // recursion with the (optional) "children" array.
    if (hdf_this.is_valid()) {
      writeAttributesIfPresent(hdf_this, *Value, Logger);
      if (auto ChildrenMaybe = find<json>("children", *Value)) {
        auto Children = *ChildrenMaybe;
        if (Children.is_array()) {
          for (auto &Child : Children) {
            createHDFStructures(&Child, hdf_this, Level + 1,
                                LinkCreationPropertyList, FixedStringHDFType,
                                HDFStreamInfo, Path, Logger);
          }
        }
      }
      Path.pop_back();
    }
  } catch (const std::exception &e) {
    // Don't throw here as the file should continue writing
    Logger->error("Failed to create structure  parent={} level={}",
                  std::string(Parent.link().path()), Level);
  }
}

void addLinks(hdf5::node::Group const &Group, nlohmann::json const &Json,
              SharedLogger Logger) {
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
    addLinks(ChildGroup, Child, Logger);
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
      Logger->warn(
          "Can not find target object for link target: {}  in group: {}",
          Target, std::string(Group.link().path()));
      continue;
    }
    if (0 > H5Olink(TargetID, static_cast<hid_t>(Group), LinkName.c_str(),
                    H5P_DEFAULT, H5P_DEFAULT)) {
      Logger->warn("can not create link name: {}  in group: {}  to target: {}",
                   LinkName, std::string(Group.link().path()), Target);
      continue;
    }
  }
}

} // namespace HDFOperations
