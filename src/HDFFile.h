#pragma once

#include "Msg.h"
#include <H5Ipublic.h>
#include <h5cpp/hdf5.hpp>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

class T_HDFFile;

namespace FileWriter {

// POD
struct StreamHDFInfo {
  std::string hdf_parent_name;
  std::string config_stream;
};

class HDFFile final {
public:
  HDFFile();

  ~HDFFile();

  void init(std::string filename, rapidjson::Value const &nexus_structure,
            rapidjson::Value const &config_file,
            std::vector<StreamHDFInfo> &stream_hdf_info, bool UseHDFSWMR);

  void init(const std::string &nexus_structure,
            std::vector<StreamHDFInfo> &stream_hdf_info);

  void init(rapidjson::Value const &nexus_structure,
            std::vector<StreamHDFInfo> &stream_hdf_info);

  void reopen(std::string filename, rapidjson::Value const &config_file);

  void flush();
  void close();

  static std::string h5_version_string_linked();
  static void write_attributes(hdf5::node::Node &node,
                               rapidjson::Value const *jsv);

  static void write_attribute_str(hdf5::node::Node &node, std::string name,
                                  std::string value);

  hdf5::file::File h5file;
  hdf5::node::Group root_group;

private:
  friend class ::T_HDFFile;
  friend class CommandHandler;

  static void check_hdf_version();
  static std::string h5_version_string_headers_compile_time();

  static void create_hdf_structures(rapidjson::Value const *value,
                                    hdf5::node::Group &parent, uint16_t level,
                                    hdf5::property::LinkCreationList lcpl,
                                    hdf5::datatype::String hdf_type_strfix,
                                    std::vector<StreamHDFInfo> &stream_hdf_info,
                                    std::deque<std::string> &path);

  static void write_hdf_ds_scalar_string(hdf5::node::Group &parent,
                                         std::string name, std::string s1);

  static void write_hdf_iso8601_now(hdf5::node::Node &node,
                                    const std::string &name);

  static void write_attributes_if_present(hdf5::node::Node &node,
                                          rapidjson::Value const *jsv);

  static std::vector<std::string> populate_strings(rapidjson::Value const *vals,
                                                   hssize_t goal_size);

  static std::vector<std::string>
  populate_fixed_strings(rapidjson::Value const *vals, size_t fix_at,
                         hssize_t goal_size);

  static void write_ds_string(hdf5::node::Group &parent, std::string name,
                              hdf5::property::DatasetCreationList &dcpl,
                              hdf5::dataspace::Dataspace &dataspace,
                              rapidjson::Value const *vals);

  static void
  write_ds_string_fixed_size(hdf5::node::Group &parent, std::string name,
                             hdf5::property::DatasetCreationList &dcpl,
                             hdf5::dataspace::Dataspace &dataspace,
                             hsize_t element_size,
                             rapidjson::Value const *vals);

  static void
  write_ds_generic(std::string const &dtype, hdf5::node::Group &parent,
                   std::string const &name, std::vector<hsize_t> const &sizes,
                   std::vector<hsize_t> const &max, hsize_t element_size,
                   rapidjson::Value const *vals);

  static void write_dataset(hdf5::node::Group &parent,
                            rapidjson::Value const *value);

  static void set_common_props(hdf5::property::FileCreationList &fcpl,
                               hdf5::property::FileAccessList &fapl) {}

  template <typename T>
  void write_hdf_ds_iso8601(hdf5::node::Group &parent, const std::string &name,
                            T &ts);

  static void writeObjectOfAttributes(hdf5::node::Node &node,
                                      const rapidjson::Value *jsv);

  static void writeArrayOfAttributes(hdf5::node::Node &Node,
                                     const rapidjson::Value *JsonValue);

  static void writeScalarAttribute(hdf5::node::Node &Node,
                                   const std::string &Name,
                                   const rapidjson::Value *AttrValue);

  static void writeAttrOfSpecifiedType(std::string const &DType,
                                       hdf5::node::Node &Node,
                                       std::string const &Name,
                                       rapidjson::Value const *Values);
};

} // namespace FileWriter
