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

  hdf5::file::File h5file;

private:
  friend class ::T_HDFFile;
  friend class CommandHandler;

  static void set_common_props(hdf5::property::FileCreationList& fcpl,
                               hdf5::property::FileAccessList& fapl) {}
};

std::string h5_version_string_linked();

void write_attributes(hid_t hdf_this, rapidjson::Value const *jsv);
void write_attributes_if_present(hid_t hdf_this, rapidjson::Value const *jsv);

} // namespace FileWriter
