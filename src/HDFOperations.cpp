// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFOperations.h"
#include "HDFAttributes.h"
#include "JsonConfig/Field.h"
#include "JsonConfig/FieldHandler.h"
#include "TimeUtility.h"
#include "json.h"
#include <stack>
#include <string>

namespace HDFOperations {
using nlohmann::json;

void findInnerSize(nlohmann::json const &JsonObj, Shape &Dimensions,
                   size_t CurrentLevel);

void findInnerSize(nlohmann::json const &JsonObj, Shape &Dimensions,
                   size_t CurrentLevel) {
  if (JsonObj.is_array()) {
    if (Dimensions.size() < CurrentLevel + 1u) {
      Dimensions.push_back(0);
    }
    if (JsonObj.size() > Dimensions.at(CurrentLevel)) {
      Dimensions.at(CurrentLevel) = JsonObj.size();
    }
    for (auto const &Element : JsonObj) {
      findInnerSize(Element, Dimensions, CurrentLevel + 1);
    }
  }
}

template <>
std::string jsonElementConverter<std::string>(nlohmann::json const &JsonObj) {
  if (JsonObj.is_string()) {
    return JsonObj.get<std::string>();
  }
  return JsonObj.dump();
}

Shape determineArrayDimensions(nlohmann::json const &Values) {
  if (not Values.is_array()) {
    return {1};
  }
  Shape ReturnDimensions;
  findInnerSize(Values, ReturnDimensions, 0);
  return ReturnDimensions;
}

template <typename T>
static void writeAttr(hdf5::node::Node const &Node, std::string const &Name,
                      nlohmann::json const &Value) {
  try {
    if (Value.is_array()) {
      HDFAttributes::writeAttribute(Node, Name,
                                    jsonArrayToMultiArray<T>(Value));
    } else {
      HDFAttributes::writeAttribute(Node, Name, Value.get<T>());
    }
  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("Failed write for numeric attribute {} in {}: {}", Name,
                    std::string(Node.link().path()), E.what())));
  }
}

void writeHDFISO8601AttributeCurrentTime(hdf5::node::Node const &Node,
                                         const std::string &Name) {
  HDFAttributes::writeAttribute(Node, Name, system_clock::now());
}

void writeAttributes(hdf5::node::Node const &Node,
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

class JSONAttribute : public JsonConfig::FieldHandler {
public:
  explicit JSONAttribute(nlohmann::json const &JsonObj) {
    processConfigData(JsonObj);
  }
  JsonConfig::RequiredField<std::string> Name{this, "name"};
  JsonConfig::RequiredField<nlohmann::json> Value{this, {"value", "values"}};
  JsonConfig::Field<std::string> Type{this, {"type", "dtype"}, "double"};
  JsonConfig::Field<size_t> StringSize{this, "string_size", 0}; // Unused
};

/// \brief Write attributes defined in an array of attribute objects.
///
/// Unlike a single attribute object this allows specifying type and dataset.
///
/// \param Node         Nodeto write attributes on.
/// \param JsonValue    json value array of attribute objects.
/// \param Logger Pointer to spdlog instance to be used for logging.
void writeArrayOfAttributes(hdf5::node::Node const &Node,
                            const nlohmann::json &ValuesJson) {
  for (auto const &Attribute : ValuesJson) {
    try {
      JSONAttribute CurrentAttribute(Attribute);
      if (Node.attributes.exists(CurrentAttribute.Name)) {
        Node.attributes.remove(CurrentAttribute.Name);
        LOG_DEBUG("Replacing (existing) attribute with key \"{}\".",
                  CurrentAttribute.Name.getValue());
      }
      if (CurrentAttribute.Type.hasDefaultValue() and
          not CurrentAttribute.Value.getValue().is_array()) {
        writeScalarAttribute(Node, CurrentAttribute.Name,
                             CurrentAttribute.Value);
      } else {
        auto CValue = CurrentAttribute.Value.getValue();
        if (CurrentAttribute.Type.hasDefaultValue() and CValue.is_array()) {
          if (std::any_of(CValue.begin(), CValue.end(),
                          [](auto &A) { return A.is_string(); })) {
            CurrentAttribute.Type.setValue("string");
          }
        }
        writeAttrOfSpecifiedType(CurrentAttribute.Type, Node,
                                 CurrentAttribute.Name, CurrentAttribute.Value);
      }
    } catch (std::exception &e) {
      LOG_ERROR("Failed to write attribute. Error was: {}", e.what());
    }
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
                              std::string const &Name,
                              nlohmann::json const &Values) {
  try {
    std::map<std::string, std::function<void()>> WriteAttrMap{
        {"uint8", [&]() { writeAttr<uint8_t>(Node, Name, Values); }},
        {"uint16", [&]() { writeAttr<uint16_t>(Node, Name, Values); }},
        {"uint32", [&]() { writeAttr<uint32_t>(Node, Name, Values); }},
        {"uint64", [&]() { writeAttr<uint64_t>(Node, Name, Values); }},
        {"int8", [&]() { writeAttr<int8_t>(Node, Name, Values); }},
        {"int16", [&]() { writeAttr<int16_t>(Node, Name, Values); }},
        {"int32", [&]() { writeAttr<int32_t>(Node, Name, Values); }},
        {"int64", [&]() { writeAttr<int64_t>(Node, Name, Values); }},
        {"float", [&]() { writeAttr<float>(Node, Name, Values); }},
        {"double", [&]() { writeAttr<double>(Node, Name, Values); }},
        {"string", [&]() { writeAttr<std::string>(Node, Name, Values); }}};
    WriteAttrMap.at(DType)();
  } catch (std::exception const &e) {
    auto ErrorStr = fmt::format(
        "Failed attribute write in {}/{} with data type {}. Message was: {}",
        std::string(Node.link().path()), Name, DType, e.what());
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
    HDFAttributes::writeAttribute(Node, Name, Values.get<std::string>());
  } else if (Values.is_number_integer()) {
    HDFAttributes::writeAttribute(Node, Name, Values.get<int64_t>());
  } else if (Values.is_number_unsigned()) {
    HDFAttributes::writeAttribute(Node, Name, Values.get<uint64_t>());
  } else if (Values.is_number_float()) {
    HDFAttributes::writeAttribute(Node, Name, Values.get<double>());
  }
}

void writeAttributesIfPresent(hdf5::node::Node const &Node,
                              nlohmann::json const &Values) {
  if (auto AttributesMaybe = find<json>("attributes", Values)) {
    auto const Attributes = *AttributesMaybe;
    writeAttributes(Node, &Attributes);
  }
}

template <typename DT>
static void writeNumericDataset(hdf5::node::Group const &Node,
                                const std::string &Name,
                                nlohmann::json const &Values) {

  try {
    auto Data = jsonArrayToMultiArray<DT>(Values);
    auto Dims = Data.getDimensions();
    auto DataSpace =
        hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end()));
    auto DCPL = hdf5::property::DatasetCreationList();
    auto Dataset = Node.create_dataset(Name, hdf5::datatype::create<DT>(),
                                       DataSpace, DCPL);
    try {

      try {
        Dataset.write(Data);
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

void writeStringDataset(hdf5::node::Group const &Parent,
                        const std::string &Name, nlohmann::json const &Values) {

  try {
    auto DataType = hdf5::datatype::String::variable();
    DataType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    DataType.padding(hdf5::datatype::StringPad::NULLTERM);
    auto StringArray = jsonArrayToMultiArray<std::string>(Values);
    auto Dims = StringArray.getDimensions();

    auto Dataspace =
        hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end()));

    Parent.create_dataset(Name, DataType, Dataspace).write(StringArray.Data);
  } catch (const std::exception &e) {
    auto ErrorStr = fmt::format(
        "Failed to write variable-size string dataset {}/{}. Message was: {}",
        std::string(Parent.link().path()), Name, e.what());
    std::throw_with_nested(std::runtime_error(ErrorStr));
  }
}

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name,
                         nlohmann::json const &Values) {
  try {
    std::map<std::string, std::function<void()>> WriteDatasetMap{
        {"uint8",
         [&]() { writeNumericDataset<uint8_t>(Parent, Name, Values); }},
        {"uint16",
         [&]() { writeNumericDataset<uint16_t>(Parent, Name, Values); }},
        {"uint32",
         [&]() { writeNumericDataset<uint32_t>(Parent, Name, Values); }},
        {"uint64",
         [&]() { writeNumericDataset<uint64_t>(Parent, Name, Values); }},
        {"int8", [&]() { writeNumericDataset<int8_t>(Parent, Name, Values); }},
        {"int16",
         [&]() { writeNumericDataset<int16_t>(Parent, Name, Values); }},
        {"int32",
         [&]() { writeNumericDataset<int32_t>(Parent, Name, Values); }},
        {"int64",
         [&]() { writeNumericDataset<int64_t>(Parent, Name, Values); }},
        {"float", [&]() { writeNumericDataset<float>(Parent, Name, Values); }},
        {"double",
         [&]() { writeNumericDataset<double>(Parent, Name, Values); }},
        {"string", [&]() { writeStringDataset(Parent, Name, Values); }},
    };
    WriteDatasetMap.at(DataType)();
  } catch (std::exception const &e) {
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Failed dataset write in {}/{}. Type={}. "
        "Message was: {}",
        std::string(Parent.link().path()), Name, DataType, e.what())));
  }
}

class JSONDataset : public JsonConfig::FieldHandler {
public:
  explicit JSONDataset(nlohmann::json const &JsonObj) {
    processConfigData(JsonObj);
  }
  JsonConfig::RequiredField<std::string> Name{this, "name"};
  JsonConfig::RequiredField<nlohmann::json> Value{this, {"value", "values"}};
  JsonConfig::Field<std::string> DataType{this, "dtype", "double"};
  JsonConfig::Field<std::string> Space{this, "space", "simple"};
  JsonConfig::Field<size_t> StringSize{this, "string_size", 0};
  JsonConfig::Field<nlohmann::json> Attributes{this, "attributes", ""};

private:
  JsonConfig::Field<std::string> Type{this, "type", ""};
  JsonConfig::Field<nlohmann::json> Size{this, "size", ""}; // Unused
};

void writeDataset(hdf5::node::Group const &Parent, const nlohmann::json *Values,
                  SharedLogger const &Logger) {

  JSONDataset Dataset(*Values);
  if (Dataset.Space.getValue() != "simple") {
    Logger->warn("Unable to handle data space of type {}. Can only handle "
                 "simple data spaces.",
                 Dataset.Space.getValue());
  }

  auto UsedDataType = Dataset.DataType.getValue();

  writeGenericDataset(UsedDataType, Parent, Dataset.Name,
                      Dataset.Value.getValue());
  auto dset = hdf5::node::Dataset(Parent.nodes[Dataset.Name]);

  writeAttributesIfPresent(dset, *Values);
}

class JSONHdfNode : public JsonConfig::FieldHandler {
public:
  explicit JSONHdfNode(nlohmann::json const &JsonObj) {
    processConfigData(JsonObj);
  }
  JsonConfig::Field<std::string> Name{this, "name", ""};
  JsonConfig::RequiredField<std::string> Type{this, "type"};
  JsonConfig::Field<nlohmann::json> Value{this, {"value", "values"}, ""};
  JsonConfig::Field<nlohmann::json> Children{this, "children", ""};
  JsonConfig::Field<std::string> DataType{this, "dtype", "int64"};
  JsonConfig::Field<std::string> Space{this, "space", "simple"};
  JsonConfig::Field<size_t> StringSize{this, "string_size", 0};
  JsonConfig::Field<nlohmann::json> Attributes{this, "attributes", ""};
  JsonConfig::Field<std::string> Target{this, "target", ""};
  JsonConfig::Field<nlohmann::json> Stream{this, "stream", ""};

private:
  JsonConfig::Field<nlohmann::json> Size{this, "size", ""}; // Unused
};

void createHDFStructures(
    const nlohmann::json *Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<StreamHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path,
    SharedLogger const &Logger) {

  try {

    // The HDF object that we will maybe create at the current level.
    JSONHdfNode CNode(*Value);
    if (CNode.Type.getValue() == "group") {
      if (CNode.Name.getValue().empty()) {
        Logger->error("HDF group name was empty/missing, ignoring.");
        return;
      }
      try {
        auto CurrentGroup =
            Parent.create_group(CNode.Name, LinkCreationPropertyList);
        Path.push_back(CNode.Name);
        writeAttributesIfPresent(CurrentGroup, *Value);
        if (not CNode.Children.hasDefaultValue() and
            CNode.Children.getValue().is_array()) {
          for (auto &Child : CNode.Children.getValue()) {
            createHDFStructures(&Child, CurrentGroup, Level + 1,
                                LinkCreationPropertyList, FixedStringHDFType,
                                HDFStreamInfo, Path, Logger);
          }
        } else {
          Logger->debug(
              "Ignoring children as they do not exist or are invalid.");
        }
        Path.pop_back();
      } catch (std::exception const &e) {
        Logger->error("Failed to create group  Name: {}. Message was: {}",
                      CNode.Name.getValue(), e.what());
      }
    } else if (CNode.Type.getValue() == "stream") {
      std::string pathstr;
      for (auto &x : Path) {
        // cppcheck-suppress useStlAlgorithm
        pathstr += "/" + x;
      }

      HDFStreamInfo.push_back(StreamHDFInfo{pathstr, Value->dump()});
    } else if (CNode.Type.getValue() == "dataset") {
      writeDataset(Parent, Value, Logger);
      writeAttributesIfPresent(Parent.get_dataset(CNode.Name.getValue()), *Value);
    } else {
      Logger->error("Unknown hdf node of type {}. Ignoring.",
                    CNode.Type.getValue());
    }
  } catch (const std::exception &e) {
    // Don't throw here as the file should continue writing
    Logger->error("Failed to create structure with path \"{}\" ({} levels "
                  "deep). Message was: {}",
                  std::string(Parent.link().path()), Level, e.what());
  }
}

void addLinks(hdf5::node::Group const &Group, nlohmann::json const &Json) {
  if (auto ChildrenMaybe = find<nlohmann::json>("children", Json)) {
    auto Children = *ChildrenMaybe;
    if (Children.is_array()) {
      for (auto &Child : Children) {
        auto NodeGroup = Group.get_group(Child.at("name").get<std::string>());
        addLinkToNode(NodeGroup, Child);
      }
    }
  }
}

void addLinkToNode(hdf5::node::Group const &Group, nlohmann::json const &Json) {
  JSONHdfNode CNode(Json);

  if (not CNode.Children.hasDefaultValue() and
      CNode.Children.getValue().is_array()) {
    for (auto &Child : CNode.Children.getValue()) {
      JSONHdfNode ChildNode(Child);
      if (ChildNode.Type.getValue() == "group") {
        auto ChildGroup = Group.get_group(Child.at("name").get<std::string>());
        addLinkToNode(ChildGroup, Child);
      } else if (ChildNode.Type.getValue() == "link" and
                 not ChildNode.Target.hasDefaultValue()) {
        auto GroupBase = Group;
        auto TargetBase = ChildNode.Target.getValue();
        while (TargetBase.find("../") == 0) {
          TargetBase = TargetBase.substr(3);
          GroupBase = GroupBase.link().parent();
        }
        auto TargetID = H5Oopen(static_cast<hid_t>(GroupBase),
                                TargetBase.c_str(), H5P_DEFAULT);
        if (TargetID < 0) {
          LOG_WARN(
              "Can not find target object for link target: {}  in group: {}",
              ChildNode.Target.getValue(), std::string(Group.link().path()));
          continue;
        }
        if (0 > H5Olink(TargetID, static_cast<hid_t>(Group),
                        CNode.Name.getValue().c_str(), H5P_DEFAULT,
                        H5P_DEFAULT)) {
          LOG_WARN("can not create link name: {}  in group: {}  to target: {}",
                   CNode.Name.getValue(), std::string(Group.link().path()),
                   ChildNode.Target.getValue());
          continue;
        }
      }
    }
  }
}

} // namespace HDFOperations
