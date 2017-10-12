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
  hid_t hdf_parent_object;
  rapidjson::Value const *config_stream;
};

class HDFFile_h5;
class HDFFile_impl;

class HDFFile final {
public:
  HDFFile();
  ~HDFFile();
  int init(std::string filename, rapidjson::Value const &nexus_structure,
           std::vector<StreamHDFInfo> &stream_hdf_info);
  void flush();
  HDFFile_h5 h5file_detail();

private:
  std::unique_ptr<HDFFile_impl> impl;
  friend class ::T_HDFFile;
};

} // namespace FileWriter
