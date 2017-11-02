#pragma once
#include "Msg.h"
#include <H5Ipublic.h>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

class T_HDFFile;

namespace FileWriter {

// Basically POD
class WriteResult {
public:
  int64_t ts;
};

class HDFFile_h5;
class HDFFile_impl;

class HDFFile final {
public:
  HDFFile();
  ~HDFFile();
  int init(std::string filename, rapidjson::Value const &nexus_structure);
  void flush();
  HDFFile_h5 h5file_detail();

private:
  std::unique_ptr<HDFFile_impl> impl;
  friend class ::T_HDFFile;
};

class FBSchemaWriter;

class FBSchemaReader {
public:
  typedef std::unique_ptr<FBSchemaReader> ptr;
  static std::unique_ptr<FBSchemaReader> create(Msg msg);
  virtual ~FBSchemaReader();
  std::unique_ptr<FBSchemaWriter> create_writer();
  bool verify(Msg msg);
  std::string sourcename(Msg msg);
  uint64_t ts(Msg msg);
  uint64_t teamid(Msg &msg);

private:
  virtual std::unique_ptr<FBSchemaWriter> create_writer_impl() = 0;
  virtual bool verify_impl(Msg msg);
  virtual std::string sourcename_impl(Msg msg) = 0;
  virtual uint64_t ts_impl(Msg msg) = 0;
  virtual uint64_t teamid_impl(Msg &msg);
};

class FBSchemaWriter {
public:
  typedef std::unique_ptr<FBSchemaWriter> ptr;
  virtual ~FBSchemaWriter();
  void init(HDFFile *hdf_file, std::string const &hdf_path,
            std::string const &sourcename, Msg msg,
            rapidjson::Value const *config_file,
            rapidjson::Document *config_stream);
  WriteResult write(Msg msg);
  void flush();
  void close();

protected:
  FBSchemaWriter() = default;
  HDFFile *hdf_file = nullptr;
  rapidjson::Value const *config_file = nullptr;
  /**
  Contains all the options set for the stream that this FBSchemaWriter
  should write to HDF.  If your plugin needs to be configurable, this is where
  you can access the options.
  */
  rapidjson::Document *config_stream = nullptr;

private:
  virtual void init_impl(std::string const &sourcename, hid_t hdf_group,
                         Msg msg) = 0;
  virtual WriteResult write_impl(Msg msg) = 0;
  hid_t hdf_group = -1;
};

} // namespace FileWriter
