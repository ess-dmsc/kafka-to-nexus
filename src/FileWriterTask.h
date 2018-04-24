#pragma once

#include "DemuxTopic.h"
#include "Source.h"
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

class Test___FileWriterTask___Create01;

namespace FileWriter {

class ParseError : public std::runtime_error {
public:
  explicit ParseError(std::string err)
      : std::runtime_error("Parse ERROR: " + err) {}
};

/**
Represents the task of writing a HDF file.
It contains the list of Source and DemuxTopic
and makes those available to the FileMaster and Streamer.
Created by Master on command message and passed to FileMaster in ctor.
*/
class FileWriterTask final {
  friend class ::Test___FileWriterTask___Create01;
  friend class CommandHandler;

public:
  FileWriterTask();
  ~FileWriterTask();
  FileWriterTask &set_hdf_filename(std::string hdf_output_prefix,
                                   std::string hdf_filename);
  /// Used by Streamer to get the list of demuxers
  std::vector<DemuxTopic> &demuxers();
  uint64_t id() const;
  std::string job_id() const;
  rapidjson::Value
  stats(rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> &a) const;
  std::string hdf_output_prefix;
  std::string hdf_filename;
  std::string filename_full;
  HDFFile hdf_file;
  bool UseHDFSWMR = false;

private:
  std::vector<DemuxTopic> _demuxers;
  void add_source(Source &&source);
  /// Called by CommandHandler on setup.
  void hdf_init(std::string const &NexusStructure,
                std::string const &ConfigFile,
                std::vector<StreamHDFInfo> &stream_hdf_info);
  void hdf_close();
  int hdf_reopen();
  void job_id_init(const std::string &);
  uint64_t _id;
  std::string _job_id;
};

} // namespace FileWriter
