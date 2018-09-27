#pragma once

#include "DemuxTopic.h"
#include "KafkaW/ProducerTopic.h"
#include "Source.h"
#include "json.h"
#include <memory>
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
  FileWriterTask(std::string ServiceID,
                 std::shared_ptr<KafkaW::ProducerTopic> StatusProducer_);
  ~FileWriterTask();
  FileWriterTask &set_hdf_filename(std::string hdf_output_prefix,
                                   std::string hdf_filename);
  /// Used by Streamer to get the list of demuxers
  std::vector<DemuxTopic> &demuxers();
  uint64_t id() const;
  std::string job_id() const;
  nlohmann::json stats() const;
  std::string hdf_output_prefix;
  std::string hdf_filename;
  std::string filename_full;
  HDFFile hdf_file;
  bool UseHDFSWMR = true;
  std::shared_ptr<KafkaW::ProducerTopic> getStatusProducer() {
    return StatusProducer;
  }

private:
  std::vector<DemuxTopic> _demuxers;
  void add_source(Source &&source);
  /// Called by CommandHandler on setup.
  void hdf_init(std::string const &NexusStructure,
                std::string const &ConfigFile,
                std::vector<StreamHDFInfo> &stream_hdf_info);
  void hdf_close_before_reopen();
  int hdf_reopen();
  void job_id_init(const std::string &);
  uint64_t _id;
  std::string JobID;
  std::string ServiceID;
  std::shared_ptr<KafkaW::ProducerTopic> StatusProducer;
};

} // namespace FileWriter
