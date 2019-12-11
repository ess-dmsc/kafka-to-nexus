// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FileWriterTask.h"
#include "EventLogger.h"
#include "HDFFile.h"
#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <atomic>
#include <thread>

namespace FileWriter {

namespace {

using nlohmann::json;

json hdf_parse(std::string const &Structure, SharedLogger Logger) {
  try {
    auto StructureDocument = json::parse(Structure);
    return StructureDocument;
  } catch (...) {
    Logger->error("Parse Error: ", Structure);
    throw FileWriter::ParseError(Structure);
  }
}
} // namespace

std::map<std::string, DemuxTopic> &FileWriterTask::demuxers() {
  return TopicNameToDemuxerMap;
}

FileWriterTask::~FileWriterTask() {
  Logger->trace("~FileWriterTask");
  TopicNameToDemuxerMap.clear();
  try {
    File.close();
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Close, ServiceId, JobId,
               "File closed");
    }
  } catch (std::exception const &E) {
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Fail, ServiceId, JobId,
               fmt::format("Exception while finishing FileWriterTask: {}",
                           E.what()));
    }
  }
}

void FileWriterTask::setFilename(std::string const &Prefix,
                                 std::string const &Name) {
  if (Prefix.empty()) {
    Filename = Name;
  } else {
    Filename = Prefix + "/" + Name;
  }
}

void FileWriterTask::addSource(Source &&Source) {
  if (swmrEnabled()) {
    Source.HDFFileForSWMR = &File;
  }

  // If demuxer does not already exist for this topic then create it
  if (TopicNameToDemuxerMap.find(Source.topic()) == TopicNameToDemuxerMap.end()) {
    TopicNameToDemuxerMap.emplace(Source.topic(), DemuxTopic(Source.topic()));
  }

  // Add the source to the demuxer for its topic
  TopicNameToDemuxerMap[Source.topic()].add_source(std::move(Source));
}

void FileWriterTask::InitialiseHdf(std::string const &NexusStructure,
                                   std::string const &ConfigFile,
                                   std::vector<StreamHDFInfo> &HdfInfo,
                                   bool UseSwmr) {
  auto NexusStructureJson = hdf_parse(NexusStructure, Logger);
  auto ConfigFileJson = hdf_parse(ConfigFile, Logger);

  try {
    Logger->info("Creating HDF file {}", Filename);
    File.init(Filename, NexusStructureJson, ConfigFileJson, HdfInfo, UseSwmr);
    // The HDF file is closed and re-opened to (optionally) support SWMR and
    // parallel writing.
    closeFile();
    reopenFile();

  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("can not initialize hdf file {}", Filename)));
  }
}

void FileWriterTask::closeFile() { File.close(); }

void FileWriterTask::reopenFile() {
  try {
    File.reopen(Filename);
  } catch (std::exception const &E) {
    Logger->error("Exception: {}", E.what());
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Error, ServiceId, JobId,
               fmt::format("Exception: {}", E.what()));
    }
    throw;
  }
}

std::string FileWriterTask::jobID() const { return JobId; }

hdf5::node::Group FileWriterTask::hdfGroup() { return File.H5File.root(); }

bool FileWriterTask::swmrEnabled() const { return File.isSWMREnabled(); }

void FileWriterTask::setJobId(std::string const &Id) { JobId = Id; }

json FileWriterTask::stats() const {
  auto Topics = json::object();
  for (auto &TopicDemuxerPair : TopicNameToDemuxerMap) {
    auto &Demux = TopicDemuxerPair.second;
    auto DemuxStats = json::object();
    DemuxStats["messages_processed"] = Demux.messages_processed.load();
    DemuxStats["error_message_too_small"] =
        Demux.error_message_too_small.load();
    DemuxStats["error_no_flatbuffer_reader"] =
        Demux.error_no_flatbuffer_reader.load();
    DemuxStats["error_no_source_instance"] =
        Demux.error_no_source_instance.load();
    Topics[Demux.topic()] = DemuxStats;
  }
  auto FWT = json::object();
  FWT["filename"] = Filename;
  FWT["topics"] = Topics;
  return FWT;
}
std::string FileWriterTask::filename() const { return Filename; }

} // namespace FileWriter
