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
#include "HDFAttributes.h"
#include "JsonConfig/FieldHandler.h"
#include "JsonConfig/Field.h"

namespace HDFOperations {
using nlohmann::json;

void findInnerSize(nlohmann::json const &JsonObj, Shape &Dimensions, size_t CurrentLevel);

void findInnerSize(nlohmann::json const &JsonObj, Shape &Dimensions, size_t CurrentLevel) {
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
static void writeAttr(hdf5::node::Node const &Node,
                             std::string const &Name,
                             nlohmann::json const &Value) {
  try {
    if (Value.is_array()) {
      HDFAttributes::writeAttribute(Node, Name, jsonArrayToMultiArray<T>(Value));
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

void writeAttributes(hdf5::node::Node const &Node, nlohmann::json const *Value) {
  if (Value == nullptr) {
    return;
  }
  if (Value->is_array()) {
    writeArrayOfAttributes(Node, *Value);
  } else if (Value->is_object()) {
    writeObjectOfAttributes(Node, *Value);
  }
}

using namespace std::string_literals;
class JSONAttribute : public JsonConfig::FieldHandler {
public:
  JSONAttribute(nlohmann::json const &JsonObj) {
    processConfigData(JsonObj);
  }
  JsonConfig::RequiredField<std::string> Name{this, "name"};
  JsonConfig::RequiredField<nlohmann::json> Value{this, "value"};
  JsonConfig::Field<std::string> Type{this, {{"type"}, {"dtype"}}, "double"};
  JsonConfig::Field<size_t> StringSize{this, "string_size", 0};
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
          LOG_DEBUG("Replacing (existing) attribute with key \"{}\".", CurrentAttribute.Name.getValue());
      }
      if (CurrentAttribute.Type.hasDefaultValue() and not CurrentAttribute.Value.getValue().is_array()) {
        writeScalarAttribute(Node, CurrentAttribute.Name, CurrentAttribute.Value);
      } else {
        auto CValue = CurrentAttribute.Value.getValue();
        if (CurrentAttribute.Type.hasDefaultValue() and CValue.is_array()) {
          if (std::any_of(CValue.begin(), CValue.end(), [](auto &A){
                return A.is_string();
              })) {
            CurrentAttribute.Type.setValue("string");
          }
        }
        writeAttrOfSpecifiedType(CurrentAttribute.Type, Node, CurrentAttribute.Name,
                                 CurrentAttribute.Value);
      }
    } catch (std::exception &e) {
      LOG_ERROR("Failed to write attribute. Error was: {}", e.what());
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
        {"string", [&]() {
          writeAttr<std::string>(Node, Name, Values);
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
static void writeNumericDataset(
    hdf5::node::Group const &Node, const std::string &Name,
    hdf5::property::DatasetCreationList const &DatasetCreationPropertyList,
    hdf5::dataspace::Dataspace const &Dataspace, const nlohmann::json *Values) {

  try {
    auto Dataset = Node.create_dataset(Name, hdf5::datatype::create<DT>(),
                                       Dataspace, DatasetCreationPropertyList);
    try {
      auto Data = jsonArrayToMultiArray<DT>(*Values);
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
    Dataset.write(jsonArrayToMultiArray<std::string>(Values),
                  DataType, Dataspace, Dataspace,
                  hdf5::property::DatasetTransferList());
  } catch (const std::exception &e) {
    auto ErrorStr =
        fmt::format("Failed to write variable-size string dataset {}/{}.",
                    std::string(Parent.link().path()), Name);
    std::throw_with_nested(std::runtime_error(ErrorStr));
  }
}

void writeGenericDataset(const std::string &DataType,
                         hdf5::node::Group const &Parent,
                         const std::string &Name,
                         const std::vector<hsize_t> &Sizes,
                         const std::vector<hsize_t> &Max,
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
         [&]() {writeStringDataset(Parent, Name, DatasetCreationList, Dataspace,
                                *Values);
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

  writeGenericDataset(DataType, Parent, Name, Sizes, Max,
                      &DatasetValuesInnerObject);
  auto dset = hdf5::node::Dataset(Parent.nodes[Name]);

  writeAttributesIfPresent(dset, *Values);
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
      writeAttributesIfPresent(hdf_this, *Value);
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
