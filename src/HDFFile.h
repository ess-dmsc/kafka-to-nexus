#pragma once
#include "Msg.h"
#include <H5Ipublic.h>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

class T_HDFFile;

namespace FileWriter {

// POD
struct StreamHDFInfo {
  // hid_t hdf_parent_object;
  std::string name;
  rapidjson::Value const *config_stream;
};

class HDFFile final {
public:
  HDFFile();
  ~HDFFile();
  int init(std::string filename, rapidjson::Value const &nexus_structure,
           rapidjson::Value const &config_file,
           std::vector<StreamHDFInfo> &stream_hdf_info,
           std::vector<hid_t> &groups);
  void flush();
  hid_t h5file = -1;
};

} // namespace FileWriter
