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
                     nlohmann::json const &Value) {
  if (Value.is_array()) {
    writeArrayOfAttributes(Node, Value);
  } else if (Value.is_object()) {
    writeObjectOfAttributes(Node, Value);
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

private:
  JsonConfig::ObsoleteField<nlohmann::json> Size{this, "size"};
  JsonConfig::ObsoleteField<size_t> StringSize{this, "string_size"};
};

/// \brief Write attributes defined in an array of attribute objects.
///
/// Unlike a single attribute object this allows specifying type and dataset.
///
/// \param Node         Nodeto write attributes on.
/// \param JsonValue    json value array of attribute objects.
void writeArrayOfAttributes(hdf5::node::Node const &Node,
                            const nlohmann::json &ValuesJson) {
  for (auto const &Attribute : ValuesJson) {
    try {
      JSONAttribute CurrentAttribute(Attribute);
      if (Node.attributes.exists(CurrentAttribute.Name)) {
        Node.attributes.remove(CurrentAttribute.Name);
        Logger::Debug(R"(Replacing (existing) attribute with key "{}".)",
                      CurrentAttribute.Name.get_value());
      }
      if (CurrentAttribute.Type.hasDefaultValue() and
          not CurrentAttribute.Value.get_value().is_array()) {
        writeScalarAttribute(Node, CurrentAttribute.Name,
                             CurrentAttribute.Value);
      } else {
        auto CValue = CurrentAttribute.Value.get_value();
        if (CurrentAttribute.Type.hasDefaultValue() and CValue.is_array()) {
          if (std::any_of(CValue.begin(), CValue.end(),
                          [](auto &A) { return A.is_string(); })) {
            CurrentAttribute.Type.setValue(std::string("dtype"),
                                           std::string("string"));
          }
        }
        writeAttrOfSpecifiedType(CurrentAttribute.Type, Node,
                                 CurrentAttribute.Name, CurrentAttribute.Value);
      }
    } catch (std::exception &e) {
      Logger::Error("Failed to write attribute. Error was: {}", e.what());
    }
  }
}

/// \brief Write scalar or array attribute of specified type.
///
/// \param DType    type of the attribute values.
/// \param Node     group or dataset to add attribute to.
/// \param Name     name of the attribute.
/// \param Values   the attribute values.
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
      Logger::Debug(R"(Replacing (existing) attribute with key "{}".)", Name);
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
    writeAttributes(Node, *AttributesMaybe);
  }
}

template <typename DT>
static void writeNumericDataset(hdf5::node::Group const &Node,
                                const std::string &Name,
                                nlohmann::json const &Values) {

  try {
    if (Values.is_number()) {
      auto Data = Values.get<DT>();
      auto DataSpace = hdf5::dataspace::Scalar();
      auto Dataset =
          Node.create_dataset(Name, hdf5::datatype::create<DT>(), DataSpace);
      Dataset.write(Data);
    } else {
      auto Data = jsonArrayToMultiArray<DT>(Values);
      auto Dims = Data.getDimensions();
      auto DataSpace =
          hdf5::dataspace::Simple(hdf5::Dimensions(Dims.begin(), Dims.end()));
      auto Dataset =
          Node.create_dataset(Name, hdf5::datatype::create<DT>(), DataSpace);
      Dataset.write(Data);
    }
  } catch (std::exception const &E) {
    throw std::runtime_error(
        fmt::format("Failed write for numeric attribute {} in {}: {}", Name,
                    std::string(Node.link().path()), E.what()));
  }
}

void writeStringDataset(hdf5::node::Group const &parent,
                        std::string const &name, std::string const &value) {
  try {
    auto data_type = hdf5::datatype::String::variable();
    data_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    data_type.padding(hdf5::datatype::StringPad::NullTerm);

    auto const dataspace = hdf5::dataspace::Scalar();
    auto dataset = parent.create_dataset(name, data_type, dataspace);
    dataset.write(value, data_type, dataspace);
  } catch (const std::exception &e) {
    auto const error_str = fmt::format(
        "Failed to write dataset for string value {}/{}. Message was: {}",
        std::string(parent.link().path()), name, e.what());
    throw std::runtime_error(error_str);
  }
}

void writeStringDataset(hdf5::node::Group const &parent,
                        std::string const &name,
                        MultiVector<std::string> const &values) {
  try {
    auto data_type = hdf5::datatype::String::variable();
    data_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    data_type.padding(hdf5::datatype::StringPad::NullTerm);

    auto const dims = values.getDimensions();
    auto dataspace =
        hdf5::dataspace::Simple(hdf5::Dimensions(dims.begin(), dims.end()));

    parent.create_dataset(name, data_type, dataspace)
        .write(values.Data, data_type, dataspace);
  } catch (std::exception const &e) {
    auto const error_str = fmt::format(
        "Failed to write variable-size string dataset {}/{}. Message was: {}",
        std::string(parent.link().path()), name, e.what());
    throw std::runtime_error(error_str);
  }
}

void writeStringDatasetFromJson(hdf5::node::Group const &Parent,
                                const std::string &Name,
                                nlohmann::json const &Values) {
  if (Values.is_array()) {
    auto StringArray = jsonArrayToMultiArray<std::string>(Values);
    writeStringDataset(Parent, Name, StringArray);
  } else {
    writeStringDataset(Parent, Name, Values.get<std::string>());
  }
}

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name,
                         nlohmann::json const &Values) {
  std::map<std::string, std::function<void()>> WriteDatasetMap{
      {"uint8", [&]() { writeNumericDataset<uint8_t>(Parent, Name, Values); }},
      {"uint16",
       [&]() { writeNumericDataset<uint16_t>(Parent, Name, Values); }},
      {"uint32",
       [&]() { writeNumericDataset<uint32_t>(Parent, Name, Values); }},
      {"uint64",
       [&]() { writeNumericDataset<uint64_t>(Parent, Name, Values); }},
      {"int8", [&]() { writeNumericDataset<int8_t>(Parent, Name, Values); }},
      {"int16", [&]() { writeNumericDataset<int16_t>(Parent, Name, Values); }},
      {"int32", [&]() { writeNumericDataset<int32_t>(Parent, Name, Values); }},
      {"int", [&]() { writeNumericDataset<int32_t>(Parent, Name, Values); }},
      {"int64", [&]() { writeNumericDataset<int64_t>(Parent, Name, Values); }},
      {"float", [&]() { writeNumericDataset<float>(Parent, Name, Values); }},
      {"double", [&]() { writeNumericDataset<double>(Parent, Name, Values); }},
      {"string", [&]() { writeStringDatasetFromJson(Parent, Name, Values); }},
  };
  WriteDatasetMap.at(DataType)();
}

class JSONDataset : public JsonConfig::FieldHandler {
public:
  explicit JSONDataset(nlohmann::json const &JsonObj) {
    processConfigData(JsonObj);
  }
  JsonConfig::RequiredField<std::string> Name{this, "name"};
  JsonConfig::RequiredField<nlohmann::json> Value{this, {"value", "values"}};
  JsonConfig::Field<std::string> DataType{this, {"type", "dtype"}, "double"};

private:
  JsonConfig::ObsoleteField<size_t> StringSize{this, "string_size"};
  JsonConfig::ObsoleteField<nlohmann::json> Size{
      this,
      "size",
  }; // Unused
};

std::string writeDataset(hdf5::node::Group const &Parent,
                         nlohmann::json const &Values) {

  JSONDataset Dataset(Values);

  auto UsedDataType = Dataset.DataType.get_value();

  writeGenericDataset(UsedDataType, Parent, Dataset.Name,
                      Dataset.Value.get_value());
  return Dataset.Name.get_value();
}

class JSONHdfNode : public JsonConfig::FieldHandler {
public:
  explicit JSONHdfNode(nlohmann::json const &JsonObj) {
    processConfigData(JsonObj);
  }
  JsonConfig::Field<std::string> Name{this, "name", ""};
  JsonConfig::RequiredField<std::string> Type{this, {"type", "module"}};
  JsonConfig::Field<nlohmann::json> Config{this, "config", ""};
  JsonConfig::Field<nlohmann::json> Children{this, "children", ""};
  JsonConfig::Field<nlohmann::json> Attributes{this, "attributes", ""};
  JsonConfig::Field<std::string> Target{this, "target", ""};

private:
  JsonConfig::ObsoleteField<nlohmann::json> Value{this, {"value", "values"}};
  JsonConfig::ObsoleteField<nlohmann::json> Stream{this, "stream"};
  JsonConfig::ObsoleteField<nlohmann::json> Size{this, "size"};
  JsonConfig::ObsoleteField<std::string> DataType{this, "dtype"};
  JsonConfig::ObsoleteField<std::string> Space{this, "space"};
  JsonConfig::ObsoleteField<size_t> StringSize{this, "string_size"};
};

void createHDFStructures(
    const nlohmann::json &Value, hdf5::node::Group const &Parent,
    uint16_t Level,
    hdf5::property::LinkCreationList const &LinkCreationPropertyList,
    hdf5::datatype::String const &FixedStringHDFType,
    std::vector<ModuleHDFInfo> &HDFStreamInfo, std::deque<std::string> &Path) {

  try {
    // The HDF object that we will maybe create at the current level.
    JSONHdfNode CNode(Value);
    if (CNode.Type.get_key() == "type") {
      if (CNode.Type.get_value() == "group") {
        if (CNode.Name.get_value().empty()) {
          Logger::Error("HDF group name was empty/missing, ignoring.");
          return;
        }
        try {
          auto groupExists = Parent.has_group(CNode.Name.get_value());
          Logger::Debug("Group {} exists: {}", CNode.Name.get_value(),
                        groupExists);
          hdf5::node::Group CurrentGroup;
          if (!groupExists) {
            CurrentGroup = Parent.create_group(CNode.Name.get_value(),
                                               LinkCreationPropertyList);
          } else {
            CurrentGroup = Parent.get_group(CNode.Name.get_value());
          }
          Path.push_back(CNode.Name.get_value());
          writeAttributesIfPresent(CurrentGroup, Value);
          if (!CNode.Children.hasDefaultValue() &&
              CNode.Children.get_value().is_array()) {
            for (auto &Child : CNode.Children.get_value()) {
              createHDFStructures(Child, CurrentGroup, Level + 1,
                                  LinkCreationPropertyList, FixedStringHDFType,
                                  HDFStreamInfo, Path);
            }
          } else {
            Logger::Debug(
                "Ignoring children as they do not exist or are invalid.");
          }
          Path.pop_back();
        } catch (std::exception const &e) {
          Logger::Error(
              "Failed to create or access group. Name: {}. Message: {}. "
              "HDF5 Path: {}",
              CNode.Name.get_value(), e.what(),
              std::string(Parent.link().path()));
        }
      } else {
        Logger::Error("Unknown HDF node of type {}. Ignoring.",
                      CNode.Type.get_value());
      }
    } else if (CNode.Type.get_key() == "module") {
      if (CNode.Type.get_value() == "dataset") {
        try {
          std::string datasetName =
              CNode.Config.get_value().at("name").get<std::string>();
          if (!Parent.has_dataset(datasetName)) {
            Logger::Debug("Creating dataset {} in group {}", datasetName,
                          std::string(Parent.link().path()));
            auto NewDataset = writeDataset(Parent, CNode.Config.get_value());
            writeAttributesIfPresent(Parent.get_dataset(NewDataset), Value);
          } else {
            Logger::Debug("Dataset {} already exists in group {}", datasetName,
                          std::string(Parent.link().path()));
            auto ExistingDataset = Parent.get_dataset(datasetName);
            writeAttributesIfPresent(ExistingDataset, Value);
          }
        } catch (const std::exception &e) {
          Logger::Error(
              "Failed to create or access dataset. Name: {}. Message: "
              "{}. HDF5 Path: {}",
              CNode.Name.get_value(), e.what(),
              std::string(Parent.link().path()));
        }
      } else {
        std::string pathstr = std::accumulate(
            Path.begin(), Path.end(), std::string{},
            [](const std::string &a, const std::string &b) -> std::string {
              return a + "/" + b;
            });

        HDFStreamInfo.push_back(ModuleHDFInfo{CNode.Type.get_value(), pathstr,
                                              CNode.Config.get_value().dump()});
      }
    }
  } catch (std::exception const &e) {
    Logger::Error(
        "Failed to create structure with path \"{}\" ({} levels deep). "
        "Message: {}",
        std::string(Parent.link().path()), Level, e.what());
  }
}

void addLinks(hdf5::node::Group const &Group,
              std::vector<ModuleSettings> const &LinkSettingsList) {
  for (auto const &LinkSettings : LinkSettingsList) {
    auto NodeGroup =
        Group.get_group(LinkSettings.ModuleHDFInfoObj.HDFParentName);
    addLinkToNode(NodeGroup, LinkSettings);
  }
}

void addLinkToNode(hdf5::node::Group const &Group,
                   ModuleSettings const &LinkSettings) {
  std::string TargetBase = LinkSettings.Source;
  std::string Name = LinkSettings.Name;
  auto GroupBase = Group;
  while (TargetBase.find("../") != std::string::npos) {
    TargetBase = TargetBase.substr(3);
    GroupBase = GroupBase.link().parent();
  }
  hid_t TargetID{};
  try {
    TargetID =
        H5Oopen(static_cast<hid_t>(GroupBase), TargetBase.c_str(), H5P_DEFAULT);
  } catch (const std::exception &e) {
    Logger::Error("Failed to open HDF5 object for link creation.");
    return;
  }
  if (TargetID < 0) {
    Logger::Info("Can not find target object for link target: {}  in group: {}",
                 Name, std::string(Group.link().path()));
  }
  if (0 > H5Olink(TargetID, static_cast<hid_t>(Group), Name.c_str(),
                  H5P_DEFAULT, H5P_DEFAULT)) {
    Logger::Info("can not create link name: {}  in group: {}  to target: {}",
                 Name, std::string(Group.link().path()), TargetBase);
  }
  try {
    H5Oclose(TargetID);
  } catch (const std::exception &e) {
    Logger::Error(
        "Could not close HDF5 object while adding link with target {} to "
        "group {}",
        TargetID, std::string(Group.link().path()));
  }
}

} // namespace HDFOperations
