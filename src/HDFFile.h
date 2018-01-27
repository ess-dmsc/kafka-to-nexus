#pragma once

#include "Msg.h"
#include <H5Ipublic.h>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>
#include <h5cpp/hdf5.hpp>

class T_HDFFile;

namespace FileWriter {

// POD
struct StreamHDFInfo {
  std::string hdf_parent_name;
  rapidjson::Value const *config_stream;
};

// Basically POD
class WriteResult {
public:
  int64_t ts;
};

class HDFFile final {
public:
  HDFFile();

  ~HDFFile();

  void init(std::string filename, rapidjson::Value const &nexus_structure,
           rapidjson::Value const &config_file,
           std::vector<StreamHDFInfo> &stream_hdf_info,
           std::vector<hid_t> &groups);

  void init(rapidjson::Value const &nexus_structure,
           std::vector<StreamHDFInfo> &stream_hdf_info,
           std::vector<hid_t> &groups);

  void reopen(std::string filename, rapidjson::Value const &config_file);


  void flush();
  void close();

  static std::string h5_version_string_linked();
  static void write_attributes(hdf5::node::Node& node, rapidjson::Value const *jsv);

  static void write_attribute_str(hdf5::node::Node& node, std::string name,
                                  std::string value);

  hdf5::file::File h5file;
  hdf5::node::Group root_group;

 private:
  friend class ::T_HDFFile;
  friend class CommandHandler;

  static void check_hdf_version();
  static std::string h5_version_string_headers_compile_time();

  static void create_hdf_structures(rapidjson::Value const *value,
                                    hdf5::node::Group& parent,
                                    uint16_t level,
                                    hdf5::property::LinkCreationList lcpl,
                                    hdf5::datatype::String hdf_type_strfix,
                                    std::vector<StreamHDFInfo> &stream_hdf_info,
                                    std::deque<std::string> &path);

  static void write_hdf_ds_scalar_string(hdf5::node::Group& parent, std::string name,
                                         std::string s1);

  static void write_hdf_iso8601_now(hdf5::node::Node& node, const std::string &name);

  static void write_attributes_if_present(hdf5::node::Node& node,
                                          rapidjson::Value const *jsv);

  static void populate_strings(std::vector<std::string> &ptrs,
                               rapidjson::Value const *vals);

  static void populate_string_pointers(std::vector<char const *> &ptrs,
                                         rapidjson::Value const *vals);

  static void populate_string_fixed_size(std::vector<char> &blob,
                                         hsize_t element_size,
                                         rapidjson::Value const *vals);

  static void write_ds_string(hdf5::node::Group& parent, std::string name,
                              std::vector<hsize_t> sizes,
                              std::vector<hsize_t> max,
                              rapidjson::Value const *vals);

  static void write_ds_string_fixed_size(hdf5::node::Group& parent, std::string name,
                                         std::vector<hsize_t> sizes,
                                         std::vector<hsize_t> max,
                                         hsize_t element_size,
                                         rapidjson::Value const *vals);

  static void write_ds_generic(std::string const &dtype,
                               hdf5::node::Group& parent, std::string const &name,
                               std::vector<hsize_t> const &sizes,
                               std::vector<hsize_t> const &max,
                               hsize_t element_size,
                               rapidjson::Value const *vals);

  static void write_dataset(hdf5::node::Group& parent, rapidjson::Value const *value);

  static void set_common_props(hdf5::property::FileCreationList& fcpl,
                               hdf5::property::FileAccessList& fapl) {}

  template <typename T>
  static void write_attribute(hdf5::node::Node& node, std::string name, T value) {
    hdf5::property::AttributeCreationList acpl;
    acpl.character_encoding(hdf5::datatype::CharacterEncoding::UTF8);
    node.attributes.create<T>(name, acpl).write(value);
  }

};

} // namespace FileWriter
