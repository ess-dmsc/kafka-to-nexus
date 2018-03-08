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
#include <nlohmann/json.hpp>

namespace FileWriter {

using std::array;
using std::string;
using std::vector;

using nlohmann::json;
using json_out_of_range = nlohmann::detail::out_of_range;

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
  } catch (std::runtime_error &) {
    LOG(Sev::Warning, "ERROR failed to detect time zone for use in ISO8601 "
                      "timestamp in HDF file")
    return;
  }
  auto now = make_zoned(current_time_zone,
                        floor<std::chrono::milliseconds>(system_clock::now()));
  write_hdf_attribute_iso8601(node, name, now);
}

void HDFFile::write_attributes(hdf5::node::Node &node,
                               rapidjson::Value const *jsv) {
  if (jsv->IsObject()) {
    for (auto &at : jsv->GetObject()) {
      if (at.value.IsString()) {
        write_attribute_str(node, at.name.GetString(), at.value.GetString());
      }
      if (at.value.IsInt64()) {
        write_attribute(node, at.name.GetString(), at.value.GetInt64());
      }
      if (at.value.IsDouble()) {
        write_attribute(node, at.name.GetString(), at.value.GetDouble());
      }
    }
  }
}

void HDFFile::write_attributes_if_present(hdf5::node::Node &node,
                                          rapidjson::Value const *jsv) {
  auto mem = jsv->FindMember("attributes");
  if (mem != jsv->MemberEnd()) {
    write_attributes(node, &mem->value);
  }
}

template <typename DT>
static std::vector<DT> populate_blob(rapidjson::Value const *vals,
                                     hssize_t goal_size) {
  std::vector<DT> ret;
  if (vals->IsInt()) {
    ret.push_back(vals->GetInt());
  } else if (vals->IsDouble()) {
    ret.push_back(vals->GetDouble());
  } else if (vals->IsArray()) {
    std::stack<rapidjson::Value const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(vals);
    ai.push(0);
    an.push(vals->GetArray().Size());

    while (not as.empty()) {
      if (as.size() > 10) {
        break;
      }
      // LOG(Sev::Error, "level: {}  ai: {}  an: {}", as.size(), ai.back(),
      // an.back());
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->GetArray()[ai.top()];
      if (v.IsArray()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        size_t n = v.GetArray().Size();
        an.push(n);
      } else if (v.IsInt()) {
        ret.push_back((DT)v.GetInt());
        ai.top()++;
      } else if (v.IsInt64()) {
        ret.push_back((DT)v.GetInt64());
        ai.top()++;
      } else if (v.IsUint64()) {
        ret.push_back((DT)v.GetUint64());
        ai.top()++;
      } else if (v.IsDouble()) {
        ret.push_back((DT)v.GetDouble());
        ai.top()++;
      }
    }
  }
  if (static_cast<hssize_t>(ret.size()) != goal_size) {
    std::stringstream ss;
    ss << "Failed to populate numeric blob ";
    ss << " size mismatch " << ret.size() << "!=" << goal_size;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  return ret;
}

std::vector<std::string> HDFFile::populate_strings(rapidjson::Value const *vals,
                                                   hssize_t goal_size) {
  std::vector<std::string> ret;
  if (vals->IsString()) {
    std::string sss(vals->GetString());
    ret.push_back(sss);
  } else if (vals->IsArray()) {
    std::stack<rapidjson::Value const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(vals);
    ai.push(0);
    an.push(vals->GetArray().Size());

    while (not as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->GetArray()[ai.top()];
      if (v.IsArray()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        size_t n = v.GetArray().Size();
        an.push(n);
      } else if (v.IsString()) {
        std::string sss(v.GetString());
        ret.push_back(sss);
        ai.top()++;
      }
    }
  }

  if (static_cast<hssize_t>(ret.size()) != goal_size) {
    std::stringstream ss;
    ss << "Failed to populate string(variable) blob ";
    ss << " size mismatch " << ret.size() << "!=" << goal_size;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  return ret;
}

std::vector<std::string>
HDFFile::populate_fixed_strings(rapidjson::Value const *vals, size_t fixed_at,
                                hssize_t goal_size) {

  if (fixed_at >= 1024 * 1024) {
    std::stringstream ss;
    ss << "Failed to allocate fixed-size string dataset ";
    ss << " bad element size: " << fixed_at;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  std::vector<std::string> ret;
  if (vals->IsString()) {
    std::string sss(vals->GetString());
    sss.resize(fixed_at, '\0');
    ret.push_back(sss);
  } else if (vals->IsArray()) {
    std::stack<rapidjson::Value const *> as;
    std::stack<size_t> ai;
    std::stack<size_t> an;
    as.push(vals);
    ai.push(0);
    an.push(vals->GetArray().Size());

    while (not as.empty()) {
      if (as.size() > 10) {
        break;
      }
      if (ai.top() >= an.top()) {
        as.pop();
        ai.pop();
        an.pop();
        continue;
      }
      auto &v = as.top()->GetArray()[ai.top()];
      if (v.IsArray()) {
        ai.top()++;
        as.push(&v);
        ai.push(0);
        size_t n = v.GetArray().Size();
        an.push(n);
      } else if (v.IsString()) {
        std::string sss(v.GetString());
        sss.resize(fixed_at, '\0');
        ret.push_back(sss);
        ai.top()++;
      }
    }
  }

  if (static_cast<hssize_t>(ret.size()) != goal_size) {
    std::stringstream ss;
    ss << "Failed to populate string(fixed) blob ";
    ss << " size mismatch " << ret.size() << "!=" << goal_size;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }

  return ret;
}

template <typename DT>
static void write_ds_numeric(hdf5::node::Group &parent, std::string name,
                             hdf5::property::DatasetCreationList &dcpl,
                             hdf5::dataspace::Dataspace &dataspace,
                             rapidjson::Value const *vals) {

  try {
    auto ds =
        parent.create_dataset(name, hdf5::datatype::create<DT>(), dataspace,
                              hdf5::property::LinkCreationList(), dcpl);
    ds.write(populate_blob<DT>(vals, dataspace.size()));
  } catch (std::exception &e) {
    std::stringstream ss;
    ss << "Failed numeric dataset write in ";
    ss << parent.link().path() << "/" << name;
    std::throw_with_nested(std::runtime_error(ss.str()));
  }
}

void HDFFile::write_ds_string(hdf5::node::Group &parent, std::string name,
                              hdf5::property::DatasetCreationList &dcpl,
                              hdf5::dataspace::Dataspace &dataspace,
                              rapidjson::Value const *vals) {

  try {
    auto dt = hdf5::datatype::String::variable();
    dt.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    dt.padding(hdf5::datatype::StringPad::NULLTERM);

    auto ds = parent.create_dataset(name, dt, dataspace,
                                    hdf5::property::LinkCreationList(), dcpl);
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
    rapidjson::Value const *vals) {

  try {
    auto dt = hdf5::datatype::String::fixed(element_size);
    dt.encoding(hdf5::datatype::CharacterEncoding::UTF8);
    dt.padding(hdf5::datatype::StringPad::NULLTERM);

    auto ds = parent.create_dataset(name, dt, dataspace,
                                    hdf5::property::LinkCreationList(), dcpl);

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
                               rapidjson::Value const *vals) {
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

void HDFFile::write_dataset(hdf5::node::Group &parent,
                            rapidjson::Value const *value) {
  std::string name;
  if (auto x = get_string(value, "name")) {
    name = x.v;
  } else {
    return;
  }

  std::string dtype = "int64";
  hsize_t element_size = H5T_VARIABLE;

  std::vector<hsize_t> sizes;
  auto ds = get_object(*value, "dataset");
  if (ds) {
    auto dso = ds.v->GetObject();
    auto ds_space = get_string(ds.v, "space");
    if (ds_space) {
      if (ds_space.v != "simple") {
        LOG(Sev::Warning, "sorry, can only handle simple data spaces");
        return;
      }
    }

    auto ds_type = get_string(ds.v, "type");
    if (ds_type) {
      dtype = ds_type.v;
    }

    // optional, default to scalar
    auto ds_size = get_array(*ds.v, "size");
    if (ds_size) {
      auto a = ds_size.v->GetArray();
      for (size_t i1 = 0; i1 < a.Size(); ++i1) {
        if (a[i1].IsInt()) {
          sizes.push_back(a[i1].GetInt());
        } else if (a[i1].IsString()) {
          if (string("unlimited") == a[i1].GetString()) {
            sizes.push_back(H5S_UNLIMITED);
          }
        }
      }
    }

    if (auto x = get_int(ds.v, "string_size")) {
      if ((x.v > 0) && (x.v != H5T_VARIABLE)) {
        element_size = x.v;
      }
    }
  }

  auto ds_values_it = value->FindMember("values");
  if (ds_values_it == value->MemberEnd()) {
    return;
  }
  auto ds_values = &ds_values_it->value;

  if (ds_values->IsDouble()) {
    dtype = "double";
  }

  auto max = sizes;
  if (not sizes.empty()) {
    if (sizes[0] == H5S_UNLIMITED) {
      if (ds_values->IsArray()) {
        sizes[0] = ds_values->GetArray().Size();
      } else {
        sizes[0] = 1;
      }
    }
  }

  auto vals = ds_values;
  write_ds_generic(dtype, parent, name, sizes, max, element_size, vals);
  auto dset = hdf5::node::Dataset(parent.nodes[name]);

  // Handle attributes on this dataset
  if (auto x = get_object(*value, "attributes"))
    write_attributes(dset, x.v);
}

void HDFFile::create_hdf_structures(rapidjson::Value const *value,
                                    hdf5::node::Group &parent, uint16_t level,
                                    hdf5::property::LinkCreationList lcpl,
                                    hdf5::datatype::String hdf_type_strfix,
                                    std::vector<StreamHDFInfo> &stream_hdf_info,
                                    std::deque<std::string> &path) {

  try {

    // The HDF object that we will maybe create at the current level.
    hdf5::node::Group hdf_this;
    if (auto type = get_string(value, "type")) {
      if (type.v == "group") {
        if (auto name = get_string(value, "name")) {

          try {
            hdf_this = parent.create_group(name.v, lcpl);
            path.push_back(name.v);
          } catch (...) {
            LOG(Sev::Critical, "failed to create group  name: {}",
                name.v.c_str());
          }
        }
      }
      if (type.v == "stream") {
        string pathstr;
        for (auto &x : path) {
          pathstr += "/" + x;
        }

        stream_hdf_info.push_back(
            StreamHDFInfo{pathstr, json_to_string(*value)});
      }
      if (type.v == "dataset") {
        write_dataset(parent, value);
      }
    }

    // If the current level in the HDF can act as a parent, then continue the
    // recursion with the (optional) "children" array.
    if (hdf_this.is_valid()) {
      write_attributes_if_present(hdf_this, value);
      auto mem = value->FindMember("children");
      if (mem != value->MemberEnd()) {
        if (mem->value.IsArray()) {
          for (auto &child : mem->value.GetArray()) {
            create_hdf_structures(&child, hdf_this, level + 1, lcpl,
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

void HDFFile::init(std::string filename,
                   rapidjson::Value const &nexus_structure,
                   rapidjson::Value const &config_file,
                   std::vector<StreamHDFInfo> &stream_hdf_info) {

  try {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    set_common_props(fcpl, fapl);
    h5file = hdf5::file::create(filename, hdf5::file::AccessFlags::EXCLUSIVE,
                                fcpl, fapl);
    init(nexus_structure, stream_hdf_info);
  } catch (std::exception &e) {
    LOG(Sev::Error,
        "ERROR could not create the HDF  path={}  file={}  trace:\n{}",
        boost::filesystem::current_path().string(), filename,
        hdf5::error::print_nested(e));
    std::throw_with_nested(std::runtime_error("HDFFile failed to open!"));
  }
}

void HDFFile::init(rapidjson::Value const &nexus_structure,
                   std::vector<StreamHDFInfo> &stream_hdf_info) {

  try {
    check_hdf_version();

    // These never gets used?!?!
    hdf5::dataspace::Scalar dsp_sc;
    hdf5::property::AttributeCreationList acpl;
    acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    hdf5::property::LinkCreationList lcpl;
    lcpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);

    auto var_string = hdf5::datatype::String::variable();
    var_string.encoding(hdf5::datatype::CharacterEncoding::UTF8);

    root_group = h5file.root();

    std::deque<std::string> path;
    if (nexus_structure.IsObject()) {
      auto value = &nexus_structure;
      auto mem = value->FindMember("children");
      if (mem != value->MemberEnd()) {
        if (mem->value.IsArray()) {
          for (auto &child : mem->value.GetArray()) {
            create_hdf_structures(&child, root_group, 0, lcpl, var_string,
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
    flush();
    h5file.close();
  } catch (std::exception &e) {
    LOG(Sev::Error, "ERROR could not close  file={}  trace:\n{}",
        h5file.id().file_name().string(), hdf5::error::print_nested(e));
    std::throw_with_nested(std::runtime_error("HDFFile failed to close!"));
  }
}

void HDFFile::reopen(std::string filename,
                     rapidjson::Value const &config_file) {
  try {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    set_common_props(fcpl, fapl);

    h5file =
        hdf5::file::open(filename, hdf5::file::AccessFlags::READWRITE, fapl);
  } catch (std::exception &e) {
    auto message = hdf5::error::print_nested(e);
    LOG(Sev::Error,
        "ERROR could not reopen HDF file  path={}  file={}  trace:\n{}",
        boost::filesystem::current_path().string(), filename, message);
    std::throw_with_nested(std::runtime_error("HDFFile failed to reopen!"));
  }
}

void HDFFile::flush() {
  try {
    h5file.flush(hdf5::file::Scope::LOCAL);
  } catch (...) {
    std::throw_with_nested(std::runtime_error("HDFFile failed to flush!"));
  }
}

} // namespace FileWriter
