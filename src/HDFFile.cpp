#include "HDFFile.h"
#include "date/date.h"
#include "helper.h"
#include "json.h"
#include "logger.h"
#include <array>
#include <chrono>
#include <ctime>
#include <deque>
#include <flatbuffers/flatbuffers.h>
#include <hdf5.h>
#include <stack>
#include <unistd.h>
#define HAS_REMOTE_API 0
#include "date/tz.h"

namespace FileWriter {

using std::array;
using std::string;
using std::vector;
using nlohmann::json;
using json_out_of_range = nlohmann::detail::out_of_range;

template <typename T>
static void write_attribute(hdf5::node::Node &node, const std::string &name,
                            T value) {
  hdf5::property::AttributeCreationList acpl;
  acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);
  node.attributes.create<T>(name, acpl).write(value);
}

template <typename T>
static void write_attribute(hdf5::node::Node &node, const std::string &name,
                            std::vector<T> values) {
  hdf5::property::AttributeCreationList acpl;
  acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);
  node.attributes.create<T>(name, {values.size()}, acpl).write(values);
}

template <typename DT>
static void append_value(nlohmann::json const *Value, std::vector<DT> &Buffer) {
  if (Value->is_number_integer()) {
    Buffer.push_back(Value->get<int64_t>());
  } else if (Value->is_number_unsigned()) {
    Buffer.push_back(Value->get<uint64_t>());
  } else if (Value->is_number_float()) {
    Buffer.push_back(Value->get<double>());
  } else {
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Expect a numeric value but got: {}", Value->dump().substr(0, 256))));
  }
}

template <typename DT>
static std::vector<DT> populate_blob(nlohmann::json const *Value,
                                     hssize_t goal_size) {
  std::vector<DT> Buffer;
  if (Value->is_array()) {
    std::stack<json const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(Value);
    ai.push(0);
    an.push(Value->size());

    while (!as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto const &v = as.top()->at(ai.top());
      if (v.is_array()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        an.push(v.size());
      } else {
        append_value(&v, Buffer);
        ai.top()++;
      }
    }
  } else {
    append_value(Value, Buffer);
  }

  if (static_cast<hssize_t>(Buffer.size()) != goal_size) {
    std::stringstream ss;
    ss << "Failed to populate numeric blob ";
    ss << " size mismatch " << Buffer.size() << "!=" << goal_size;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  return Buffer;
}

template <typename T>
static void writeAttrNumeric(hdf5::node::Node &Node, const std::string &Name,
                             nlohmann::json const *Value) {
  hssize_t Length = 1;
  if (Value->is_array()) {
    Length = Value->size();
  }
  try {
    auto ValueData = populate_blob<T>(Value, Length);
    try {
      if (Value->is_array()) {
        write_attribute(Node, Name, ValueData);
      } else {
        write_attribute(Node, Name, ValueData[0]);
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

herr_t visitor_show_name(hid_t oid, char const *name, H5O_info_t const *oi,
                         void *op_data) {
  LOG(Sev::Error, "obj refs: {:2}  name: {}", oi->rc, name);
  return 0;
}

HDFFile::~HDFFile() {
  try {
    finalize();
    close();
  }
  // we do this to prevent destructor from throwing
  catch (std::exception &e) {
    LOG(Sev::Error, "HDFFile failed to close, stack:\n{}",
        hdf5::error::print_nested(e));
  }
}

void HDFFile::write_hdf_ds_scalar_string(hdf5::node::Group &parent,
                                         std::string name, std::string s1) {

  auto strfix = hdf5::datatype::String::variable();
  strfix.encoding(hdf5::datatype::CharacterEncoding::UTF8);

  auto dsp = hdf5::dataspace::Scalar();
  auto ds = parent.create_dataset(name, strfix, dsp);
  ds.write(s1, strfix, dsp, dsp);
}

void HDFFile::write_attribute_str(hdf5::node::Node &node, std::string name,
                                  std::string value) {
  auto string_type = hdf5::datatype::String::variable();
  string_type.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  hdf5::property::AttributeCreationList acpl;
  acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

  auto at = node.attributes.create(name, string_type, hdf5::dataspace::Scalar(),
                                   acpl);
  at.write(value, string_type);
}

template <typename T>
void HDFFile::write_hdf_ds_iso8601(hdf5::node::Group &parent,
                                   const std::string &name, T &ts) {
  using namespace date;
  using namespace std::chrono;
  auto s2 = format("%Y-%m-%dT%H:%M:%S%z", ts);
  HDFFile::write_hdf_ds_scalar_string(parent, name, s2);
}

template <typename T>
static void write_hdf_attribute_iso8601(hdf5::node::Node &node,
                                        std::string name, T &ts) {
  using namespace date;
  using namespace std::chrono;
  auto s2 = format("%Y-%m-%dT%H:%M:%S%z", ts);
  HDFFile::write_attribute_str(node, name, s2);
}

void HDFFile::write_hdf_iso8601_now(hdf5::node::Node &node,
                                    const std::string &name) {
  using namespace date;
  using namespace std::chrono;
  const time_zone *current_time_zone;
  try {
    current_time_zone = current_zone();
  } catch (std::runtime_error const &e) {
    LOG(Sev::Warning, "ERROR failed to detect time zone for use in ISO8601 "
                      "timestamp in HDF file")
    current_time_zone = locate_zone("UTC");
  }
  auto now = make_zoned(current_time_zone,
                        floor<std::chrono::milliseconds>(system_clock::now()));
  write_hdf_attribute_iso8601(node, name, now);
}

void HDFFile::write_attributes(hdf5::node::Node &Node,
                               nlohmann::json const *Value) {
  if (Value->is_array()) {
    writeArrayOfAttributes(Node, Value);
  } else if (Value->is_object()) {
    writeObjectOfAttributes(Node, Value);
  }
}

/// Write attributes defined in an array of attribute objects
/// Unlike a single attribute object this allows specifying type and dataset
/// \param Node : node to write attributes on
/// \param JsonValue : json value array of attribute objects
void HDFFile::writeArrayOfAttributes(hdf5::node::Node &Node,
                                     nlohmann::json const *Value) {
  if (!Value->is_array()) {
    return;
  }
  for (auto const &Attribute : *Value) {
    if (Attribute.is_object()) {
      string Name;
      if (auto NameMaybe = find<std::string>("name", Attribute)) {
        Name = NameMaybe.inner();
      } else {
        continue;
      }
      if (auto ValuesMaybe = find<json>("values", Attribute)) {
        std::string DType;
        auto const &Values = ValuesMaybe.inner();
        if (auto AttrType = find<std::string>("type", Attribute)) {
          DType = AttrType.inner();
          writeAttrOfSpecifiedType(DType, Node, Name, &Values);
        } else {
          if (Values.is_array()) {
            LOG(Sev::Warning, "Attributes with array values must specify type")
            continue;
          }
          writeScalarAttribute(Node, Name, &Values);
        }
      }
    }
  }
}

/// Write scalar or array attribute of specfied type
/// \param DType : type of the attribute values
/// \param Node : group or dataset to add attribute to
/// \param Name : name of the attribute
/// \param Values : the attribute values
void HDFFile::writeAttrOfSpecifiedType(std::string const &DType,
                                       hdf5::node::Node &Node,
                                       std::string const &Name,
                                       nlohmann::json const *Values) {
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
      if (Values->is_array()) {
        auto ValueArray = populate_strings(Values, Values->size());
        auto StringAttr =
            Node.attributes.create(Name, hdf5::datatype::create<std::string>(),
                                   hdf5::dataspace::Simple{{Values->size()}});
        StringAttr.write(ValueArray);
      } else {
        std::string const StringValue = Values->get<std::string>();
        auto string_type = hdf5::datatype::String::fixed(StringValue.size());
        auto StringAttr = Node.attributes.create(Name, string_type,
                                                 hdf5::dataspace::Scalar());
        StringAttr.write(StringValue, string_type);
      }
    }
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Failed attribute write in ";
    ss << Node.link().path() << "/" << Name;
    ss << " type='" << DType << "'";
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

/// Write attributes defined in an object of name-value pairs
/// \param node : node to write attributes on
/// \param jsv : json value object of attributes
void HDFFile::writeObjectOfAttributes(hdf5::node::Node &Node,
                                      nlohmann::json const *Value) {
  for (auto It = Value->begin(); It != Value->end(); ++It) {
    auto const Name = It.key();
    writeScalarAttribute(Node, Name, &It.value());
  }
}

/// Write a scalar attribute when the type is to be inferred
/// \param Node : Group or dataset to write attribute to
/// \param Name : Name of the attribute
/// \param AttrValue : Json value containing the attribute value
void HDFFile::writeScalarAttribute(hdf5::node::Node &Node,
                                   const std::string &Name,
                                   nlohmann::json const *AttrValue) {
  if (AttrValue->is_string()) {
    write_attribute_str(Node, Name, AttrValue->get<std::string>());
  } else if (AttrValue->is_number_integer()) {
    write_attribute(Node, Name, AttrValue->get<int64_t>());
  } else if (AttrValue->is_number_unsigned()) {
    write_attribute(Node, Name, AttrValue->get<uint64_t>());
  } else if (AttrValue->is_number_float()) {
    write_attribute(Node, Name, AttrValue->get<double>());
  }
}

void HDFFile::write_attributes_if_present(hdf5::node::Node &Node,
                                          nlohmann::json const *Value) {
  if (auto AttributesMaybe = find<json>("attributes", *Value)) {
    auto const Attributes = AttributesMaybe.inner();
    write_attributes(Node, &Attributes);
  }
}

std::vector<std::string> HDFFile::populate_strings(nlohmann::json const *Value,
                                                   hssize_t goal_size) {
  std::vector<std::string> Buffer;
  if (Value->is_string()) {
    auto String = Value->get<std::string>();
    Buffer.push_back(String);
  } else if (Value->is_array()) {
    std::stack<json const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(Value);
    ai.push(0);
    an.push(Value->size());

    while (!as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->at(ai.top());
      if (v.is_array()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        an.push(v.size());
      } else if (v.is_string()) {
        Buffer.push_back(v.get<std::string>());
        ai.top()++;
      }
    }
  }

  if (static_cast<hssize_t>(Buffer.size()) != goal_size) {
    std::stringstream ss;
    ss << "Failed to populate string(variable) blob ";
    ss << " size mismatch " << Buffer.size() << "!=" << goal_size;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  return Buffer;
}

std::vector<std::string>
HDFFile::populate_fixed_strings(nlohmann::json const *Value, size_t FixedAt,
                                hssize_t goal_size) {
  if (FixedAt >= 1024 * 1024) {
    std::throw_with_nested(std::runtime_error(fmt::format(
        "Failed to allocate fixed-size string dataset, bad element size: {}",
        FixedAt)));
  }

  std::vector<std::string> Buffer;
  if (Value->is_string()) {
    auto String = Value->get<std::string>();
    String.resize(FixedAt, '\0');
    Buffer.push_back(String);
  } else if (Value->is_array()) {
    std::stack<json const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(Value);
    ai.push(0);
    an.push(Value->size());

    while (!as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->at(ai.top());
      if (v.is_array()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        an.push(v.size());
      } else if (v.is_string()) {
        auto String = v.get<std::string>();
        String.resize(FixedAt, '\0');
        Buffer.push_back(String);
        ai.top()++;
      }
    }
  }

  if (static_cast<hssize_t>(Buffer.size()) != goal_size) {
    std::stringstream ss;
    ss << "Failed to populate string(fixed) blob ";
    ss << " size mismatch " << Buffer.size() << "!=" << goal_size;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  return Buffer;
}

template <typename DT>
static void write_ds_numeric(hdf5::node::Group &Node, std::string Name,
                             hdf5::property::DatasetCreationList &DCPL,
                             hdf5::dataspace::Dataspace &Dataspace,
                             nlohmann::json const *Values) {

  try {
    auto ds = Node.create_dataset(Name, hdf5::datatype::create<DT>(), Dataspace,
                                  DCPL);
    try {
      auto Blob = populate_blob<DT>(Values, Dataspace.size());
      try {
        ds.write(Blob);
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

void HDFFile::write_ds_string(hdf5::node::Group &parent, std::string name,
                              hdf5::property::DatasetCreationList &dcpl,
                              hdf5::dataspace::Dataspace &dataspace,
                              nlohmann::json const *vals) {

  try {
    auto dt = hdf5::datatype::String::variable();
    dt.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    dt.padding(hdf5::datatype::StringPad::NULLTERM);

    auto ds = parent.create_dataset(name, dt, dataspace, dcpl);
    ds.write(populate_strings(vals, dataspace.size()), dt, dataspace, dataspace,
             hdf5::property::DatasetTransferList());
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Failed to write variable-size string dataset ";
    ss << parent.link().path() << "/" << name;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

void HDFFile::write_ds_string_fixed_size(
    hdf5::node::Group &parent, std::string name,
    hdf5::property::DatasetCreationList &dcpl,
    hdf5::dataspace::Dataspace &dataspace, hsize_t element_size,
    nlohmann::json const *vals) {

  try {
    auto dt = hdf5::datatype::String::fixed(element_size);
    dt.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    dt.padding(hdf5::datatype::StringPad::NULLTERM);

    auto ds = parent.create_dataset(name, dt, dataspace, dcpl);

    dt.padding(hdf5::datatype::StringPad::NULLPAD);
    ds.write(populate_fixed_strings(vals, element_size, dataspace.size()), dt,
             dataspace, dataspace, hdf5::property::DatasetTransferList());

  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Failed to write fixed-size string dataset ";
    ss << parent.link().path() << "/" << name;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

void HDFFile::write_ds_generic(std::string const &dtype,
                               hdf5::node::Group &parent,
                               std::string const &name,
                               std::vector<hsize_t> const &sizes,
                               std::vector<hsize_t> const &max,
                               hsize_t element_size,
                               nlohmann::json const *vals) {
  try {

    hdf5::property::DatasetCreationList dcpl;
    hdf5::dataspace::Dataspace dataspace = hdf5::dataspace::Scalar();
    if (!sizes.empty()) {
      dataspace = hdf5::dataspace::Simple(sizes, max);
      if (max[0] == H5S_UNLIMITED) {
        dcpl.chunk(sizes);
      }
    }

    if (dtype == "uint8") {
      write_ds_numeric<uint8_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "uint16") {
      write_ds_numeric<uint16_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "uint32") {
      write_ds_numeric<uint32_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "uint64") {
      write_ds_numeric<uint64_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "int8") {
      write_ds_numeric<int8_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "int16") {
      write_ds_numeric<int16_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "int32") {
      write_ds_numeric<int32_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "int64") {
      write_ds_numeric<int64_t>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "float") {
      write_ds_numeric<float>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "double") {
      write_ds_numeric<double>(parent, name, dcpl, dataspace, vals);
    }
    if (dtype == "string") {
      // TODO
      // We currently not support fixed length strings, apparently some
      // to-be-tracked issue with h5cpp at the moment.
      if (true || element_size == H5T_VARIABLE) {
        write_ds_string(parent, name, dcpl, dataspace, vals);
      } else {
        write_ds_string_fixed_size(parent, name, dcpl, dataspace, element_size,
                                   vals);
      }
    }
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Failed dataset write in ";
    ss << parent.link().path() << "/" << name;
    ss << " type='" << dtype << "'";
    ss << " size(";
    for (auto s : sizes)
      ss << s << " ";
    ss << ")  max(";
    for (auto s : max)
      ss << s << " ";
    ss << ")  ";
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

void HDFFile::write_dataset(hdf5::node::Group &Parent,
                            nlohmann::json const *Value) {
  std::string Name;
  if (auto NameMaybe = find<std::string>("name", *Value)) {
    Name = NameMaybe.inner();
  } else {
    return;
  }

  std::string DataType = "int64";
  hsize_t element_size = H5T_VARIABLE;

  std::vector<hsize_t> sizes;
  if (auto ds = find<json>("dataset", *Value)) {
    auto dso = ds.inner();
    if (auto ds_space = find<std::string>("space", dso)) {
      if (ds_space.inner() != "simple") {
        LOG(Sev::Warning, "sorry, can only handle simple data spaces");
        return;
      }
    }

    if (auto ds_type = find<std::string>("type", dso)) {
      DataType = ds_type.inner();
    }

    // optional, default to scalar
    if (auto ds_size = find<json>("size", dso)) {
      if (ds_size.inner().is_array()) {
        auto a = ds_size.inner();
        for (auto const &Element : a) {
          if (Element.is_number_integer()) {
            sizes.push_back(Element.get<int64_t>());
          } else if (Element.is_string()) {
            if (Element.get<std::string>() == "unlimited") {
              sizes.push_back(H5S_UNLIMITED);
            }
          }
        }
      }
    }

    if (auto x = find<uint64_t>("string_size", dso)) {
      if ((x.inner() > 0) && (x.inner() != H5T_VARIABLE)) {
        element_size = x.inner();
      }
    }
  }

  auto ds_values_maybe = find<json>("values", *Value);
  if (!ds_values_maybe) {
    return;
  }
  auto ds_values = ds_values_maybe.inner();

  if (ds_values.is_number_float()) {
    DataType = "double";
  }

  auto max = sizes;
  if (!sizes.empty()) {
    if (sizes[0] == H5S_UNLIMITED) {
      if (ds_values.is_array()) {
        sizes[0] = ds_values.size();
      } else {
        sizes[0] = 1;
      }
    }
  }

  write_ds_generic(DataType, Parent, Name, sizes, max, element_size,
                   &ds_values);
  auto dset = hdf5::node::Dataset(Parent.nodes[Name]);

  write_attributes_if_present(dset, Value);
}

void HDFFile::create_hdf_structures(nlohmann::json const *value,
                                    hdf5::node::Group &parent, uint16_t level,
                                    hdf5::property::LinkCreationList lcpl,
                                    hdf5::datatype::String hdf_type_strfix,
                                    std::vector<StreamHDFInfo> &stream_hdf_info,
                                    std::deque<std::string> &path) {

  try {

    // The HDF object that we will maybe create at the current level.
    hdf5::node::Group hdf_this;
    if (auto TypeMaybe = find<std::string>("type", *value)) {
      auto Type = TypeMaybe.inner();
      if (Type == "group") {
        if (auto NameMaybe = find<std::string>("name", *value)) {
          auto Name = NameMaybe.inner();
          try {
            hdf_this = parent.create_group(Name, lcpl);
            path.push_back(Name);
          } catch (...) {
            LOG(Sev::Critical, "failed to create group  Name: {}", Name);
          }
        }
      }
      if (Type == "stream") {
        string pathstr;
        for (auto &x : path) {
          pathstr += "/" + x;
        }

        stream_hdf_info.push_back(StreamHDFInfo{pathstr, value->dump()});
      }
      if (Type == "dataset") {
        write_dataset(parent, value);
      }
    }

    // If the current level in the HDF can act as a parent, then continue the
    // recursion with the (optional) "children" array.
    if (hdf_this.is_valid()) {
      write_attributes_if_present(hdf_this, value);
      if (auto ChildrenMaybe = find<json>("children", *value)) {
        auto Children = ChildrenMaybe.inner();
        if (Children.is_array()) {
          for (auto &Child : Children) {
            create_hdf_structures(&Child, hdf_this, level + 1, lcpl,
                                  hdf_type_strfix, stream_hdf_info, path);
          }
        }
      }
      path.pop_back();
    }
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Failed to create structure  parent=";
    ss << parent.link().path() << "  level=" << level;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

/// Human readable version of the HDF5 headers that we compile against.
std::string HDFFile::h5_version_string_headers_compile_time() {
  return fmt::format("{}.{}.{}", H5_VERS_MAJOR, H5_VERS_MINOR, H5_VERS_RELEASE);
}

/// Human readable version of the HDF5 libraries that we run with.
std::string HDFFile::h5_version_string_linked() {
  unsigned h5_vers_major, h5_vers_minor, h5_vers_release;
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  return fmt::format("{}.{}.{}", h5_vers_major, h5_vers_minor, h5_vers_release);
}

/// Compare the version of the HDF5 headers which the kafka-to-nexus was
/// compiled with against the version of the HDF5 libraries that the
/// kafka-to-nexus is linked against at runtime. Currently, a mismatch in the
/// release number is logged but does not cause panic.
void HDFFile::check_hdf_version() {
  unsigned h5_vers_major, h5_vers_minor, h5_vers_release;
  H5get_libversion(&h5_vers_major, &h5_vers_minor, &h5_vers_release);
  if (h5_vers_major != H5_VERS_MAJOR) {
    LOG(Sev::Error, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        h5_version_string_headers_compile_time(), h5_version_string_linked());
    exit(1);
  }
  if (h5_vers_minor != H5_VERS_MINOR) {
    LOG(Sev::Error, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        h5_version_string_headers_compile_time(), h5_version_string_linked());
    exit(1);
  }
  if (h5_vers_release != H5_VERS_RELEASE) {
    LOG(Sev::Error, "HDF5 version mismatch.  compile time: {}  runtime: {}",
        h5_version_string_headers_compile_time(), h5_version_string_linked());
  }
}

extern "C" char const GIT_COMMIT[];

void HDFFile::init(std::string Filename, nlohmann::json const &NexusStructure,
                   nlohmann::json const &config_file,
                   std::vector<StreamHDFInfo> &stream_hdf_info,
                   bool UseHDFSWMR) {
  if (std::ifstream(Filename).good()) {
    // File exists already
    throw std::runtime_error(
        fmt::format("The file \"{}\" exists already.", Filename));
  }
  try {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    set_common_props(fcpl, fapl);
    if (UseHDFSWMR) {
      h5file =
          hdf5::file::create(Filename, hdf5::file::AccessFlags::TRUNCATE |
                                           hdf5::file::AccessFlags::SWMR_WRITE,
                             fcpl, fapl);
      isSWMREnabled_ = true;
    } else {
      h5file = hdf5::file::create(Filename, hdf5::file::AccessFlags::EXCLUSIVE,
                                  fcpl, fapl);
    }
    this->Filename = Filename;
    init(NexusStructure, stream_hdf_info);
  } catch (std::exception &e) {
    LOG(Sev::Error,
        "ERROR could not create the HDF  path={}  file={}  trace:\n{}",
        boost::filesystem::current_path().string(), Filename,
        hdf5::error::print_nested(e));
    std::throw_with_nested(std::runtime_error("HDFFile failed to open!"));
  }
  this->NexusStructure = NexusStructure;
}

void HDFFile::init(const std::string &nexus_structure,
                   std::vector<StreamHDFInfo> &stream_hdf_info) {
  auto Document = nlohmann::json::parse(nexus_structure);
  init(Document, stream_hdf_info);
}

void HDFFile::init(nlohmann::json const &nexus_structure,
                   std::vector<StreamHDFInfo> &stream_hdf_info) {

  try {
    check_hdf_version();

    hdf5::property::AttributeCreationList acpl;
    acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    hdf5::property::LinkCreationList lcpl;
    lcpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    auto var_string = hdf5::datatype::String::variable();
    var_string.encoding(hdf5::datatype::CharacterEncoding::UTF8);

    root_group = h5file.root();

    std::deque<std::string> path;
    if (nexus_structure.is_object()) {
      auto value = &nexus_structure;
      if (auto ChildrenMaybe = find<json>("children", *value)) {
        auto Children = ChildrenMaybe.inner();
        if (Children.is_array()) {
          for (auto &Child : Children) {
            create_hdf_structures(&Child, root_group, 0, lcpl, var_string,
                                  stream_hdf_info, path);
          }
        }
      }
    }

    write_attribute_str(root_group, "HDF5_Version", h5_version_string_linked());
    write_attribute_str(root_group, "file_name",
                        h5file.id().file_name().stem().string());
    write_attribute_str(root_group, "creator",
                        fmt::format("kafka-to-nexus commit {:.7}", GIT_COMMIT));
    write_hdf_iso8601_now(root_group, "file_time");
    write_attributes_if_present(root_group, &nexus_structure);
  } catch (std::exception &e) {
    LOG(Sev::Critical, "ERROR could not initialize  file={}  trace:\n{}",
        h5file.id().file_name().string(), hdf5::error::print_nested(e));
    std::throw_with_nested(std::runtime_error("HDFFile failed to initialize!"));
  }
}

void HDFFile::close() {
  try {
    if (h5file.is_valid()) {
      LOG(Sev::Debug, "flushing");
      flush();
      LOG(Sev::Debug, "closing");
      h5file.close();
      LOG(Sev::Debug, "closed");
      // Make sure that h5file.is_valid() == false from now on:
      h5file = hdf5::file::File();
    }
  } catch (std::exception const &E) {
    auto Trace = hdf5::error::print_nested(E);
    LOG(Sev::Error, "ERROR could not close  file={}  trace:\n{}",
        h5file.id().file_name().string(), Trace);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "HDFFile failed to close.  Current Path: {}  Filename: {}  Trace:\n{}",
        boost::filesystem::current_path().string(),
        h5file.id().file_name().string(), Trace)));
  }
}

void HDFFile::reopen(std::string filename) {
  try {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    set_common_props(fcpl, fapl);

    hdf5::file::AccessFlagsBase FAFL = static_cast<hdf5::file::AccessFlagsBase>(
        hdf5::file::AccessFlags::READWRITE);
    if (isSWMREnabled_) {
      FAFL |= static_cast<hdf5::file::AccessFlagsBase>(
          hdf5::file::AccessFlags::SWMR_WRITE);
    }
    h5file = hdf5::file::open(filename, FAFL, fapl);
  } catch (std::exception const &E) {
    auto Trace = hdf5::error::print_nested(E);
    LOG(Sev::Error,
        "ERROR could not reopen HDF file  path={}  file={}  trace:\n{}",
        boost::filesystem::current_path().string(), filename, Trace);
    std::throw_with_nested(std::runtime_error(fmt::format(
        "HDFFile failed to reopen.  Current Path: {}  Filename: {}  Trace:\n{}",
        boost::filesystem::current_path().string(), filename, Trace)));
  }
}

void HDFFile::flush() {
  try {
    if (h5file.is_valid()) {
      h5file.flush(hdf5::file::Scope::GLOBAL);
    }
  } catch (std::runtime_error const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("HDFFile failed to flush  what: {}", E.what())));
  } catch (...) {
    std::throw_with_nested(
        std::runtime_error("HDFFile failed to flush with unknown exception"));
  }
}

static void addLinks(hdf5::node::Group &Group, nlohmann::json const &Json) {
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
      LOG(Sev::Warning,
          "Can not find target object for link target: {}  in group: {}",
          Target, std::string(Group.link().path()));
      continue;
    }
    if (0 > H5Olink(TargetID, static_cast<hid_t>(Group), LinkName.c_str(),
                    H5P_DEFAULT, H5P_DEFAULT)) {
      LOG(Sev::Warning,
          "can not create link name: {}  in group: {}  to target: {}", LinkName,
          std::string(Group.link().path()), Target);
      continue;
    }
  }
}

void HDFFile::finalize() {
  if (Filename.empty()) {
    LOG(Sev::Debug, "File was never opened, skip finalize()");
  }
  try {
    if (h5file.is_valid()) {
      close();
    }
    hdf5::property::FileCreationList FCPL;
    hdf5::property::FileAccessList FAPL;
    set_common_props(FCPL, FAPL);
    hdf5::file::AccessFlagsBase FAFL = static_cast<hdf5::file::AccessFlagsBase>(
        hdf5::file::AccessFlags::READWRITE);
    h5file = hdf5::file::open(Filename, FAFL, FAPL);
    auto Group = h5file.root();
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

bool HDFFile::isSWMREnabled() const { return isSWMREnabled_; }

} // namespace FileWriter
