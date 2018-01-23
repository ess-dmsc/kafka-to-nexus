#pragma once

#include "Alloc.h"
#include "CollectiveQueue.h"
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
  int init(std::string filename, rapidjson::Value const &nexus_structure,
           rapidjson::Value const &config_file,
           std::vector<StreamHDFInfo> &stream_hdf_info,
           std::vector<hid_t> &groups);
  int close();
  int reopen(std::string filename, rapidjson::Value const &config_file);
  int init(hid_t h5file, std::string filename, rapidjson::Value const &nexus_structure,
           std::vector<StreamHDFInfo> &stream_hdf_info, std::vector<hid_t> &groups);
  void flush();
#if USE_PARALLEL_WRITER
  void create_collective_queue(Jemalloc::sptr jm);
#endif
  hid_t h5file = -1;
  std::string filename;
  CollectiveQueue *cq;

private:
  friend class ::T_HDFFile;
  friend class CommandHandler;
};

std::string h5_version_string_linked();

void write_attributes(hid_t hdf_this, rapidjson::Value const *jsv);
void write_attributes_if_present(hid_t hdf_this, rapidjson::Value const *jsv);

} // namespace FileWriter
