#pragma once
#include "DemuxTopic.h"
#include "Source.h"
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

class Test___FileWriterTask___Create01;

namespace FileWriter {

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
  FileWriterTask &set_hdf_filename(std::string hdf_filename);
  /// Used by Streamer to get the list of demuxers
  std::vector<DemuxTopic> &demuxers();
  uint64_t id() const;
  std::string jobid() const;
  rapidjson::Value
  stats(rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> &a) const;
  std::string hdf_filename;
  HDFFile hdf_file;

private:
  std::vector<DemuxTopic> _demuxers;
  void add_source(Source &&source);
  /// Called by CommandHandler on setup.
  int hdf_init(rapidjson::Value const &nexus_structure,
               std::vector<StreamHDFInfo> &stream_hdf_info);
  void jobid_init(const std::string&);
  uint64_t _id;
  std::string _jobid;
};

} // namespace FileWriter
