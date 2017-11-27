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
           std::vector<StreamHDFInfo> &stream_hdf_info);
  int init(hid_t h5file, std::string filename,
           rapidjson::Value const &nexus_structure,
           std::vector<StreamHDFInfo> &stream_hdf_info);
  void flush();
  hid_t h5file = -1;

private:
  friend class ::T_HDFFile;
  friend class CommandHandler;
};

void write_attributes(hid_t hdf_this, rapidjson::Value const *jsv);
void write_attributes_if_present(hid_t hdf_this, rapidjson::Value const *jsv);

} // namespace FileWriter
