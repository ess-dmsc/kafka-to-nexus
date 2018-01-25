#pragma once

#include "DemuxTopic.h"
#include "Source.h"
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <vector>

#if USE_PARALLEL_WRITER
#include "CollectiveQueue.h"
#include <mpi.h>
#endif

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

#if USE_PARALLEL_WRITER
  void mpi_start(std::vector<MPIChild::ptr> &&to_spawn);
  void mpi_stop();
  size_t spawned = -1;
  MPI_Comm comm_all;
  MPI_Comm comm_spawned;
  HDFIDStore hdf_store;
  CollectiveQueue::ptr cq;
#endif

private:
  std::vector<DemuxTopic> _demuxers;
  void add_source(Source &&source);
  /// Called by CommandHandler on setup.
  int hdf_init(rapidjson::Value const &nexus_structure,
               rapidjson::Value const &config_file,
               std::vector<StreamHDFInfo> &stream_hdf_info,
               std::vector<hid_t> &groups);
  int hdf_close();
  int hdf_reopen();
  void job_id_init(const std::string &);
  uint64_t _id;
  std::string _job_id;
};

} // namespace FileWriter
