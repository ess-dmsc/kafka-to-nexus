// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FileWriterTask.h"
#include "DemuxTopic.h"
#include "HDFFile.h"
#include "KafkaW/ProducerTopic.h"
#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <atomic>

namespace FileWriter {

namespace {

using nlohmann::json;

json hdf_parse(std::string const &Structure, SharedLogger const &Logger) {
  try {
    auto StructureDocument = json::parse(Structure);
    return StructureDocument;
  } catch (...) {
    Logger->error("Parse Error: ", Structure);
    throw FileWriter::ParseError(Structure);
  }
}
} // namespace

std::map<std::string, std::shared_ptr<DemuxTopic>> &FileWriterTask::demuxers() {
  return TopicNameToDemuxerMap;
}

FileWriterTask::~FileWriterTask() {
  Logger->trace("~FileWriterTask");
  TopicNameToDemuxerMap.clear();
  try {
    closeFile();
  } catch (std::exception const &E) {
    Logger->error(fmt::format(
        "Exception while closing file in ~FileWriterTask: {}", E.what()));
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
    Source.HDFFileForSWMR = File;
  }

  TopicNameToDemuxerMap.emplace(Source.topic(),
                                std::make_shared<DemuxTopic>(Source.topic()));

  // Add the source to the demuxer for its topic
  TopicNameToDemuxerMap[Source.topic()]->addSource(std::move(Source));
}

void FileWriterTask::InitialiseHdf(std::string const &NexusStructure,
                                   std::vector<StreamHDFInfo> &HdfInfo,
                                   bool UseSwmr) {
  auto NexusStructureJson = hdf_parse(NexusStructure, Logger);

  try {
    Logger->info("Creating HDF file {}", Filename);
    File->init(Filename, NexusStructureJson, HdfInfo, UseSwmr);
    // The HDF file is closed and re-opened to (optionally) support SWMR and
    // parallel writing.
    closeFile();
    reopenFile();

  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(
        fmt::format("can not initialize hdf file {}", Filename)));
  }
}

void FileWriterTask::closeFile() { File->close(); }

void FileWriterTask::reopenFile() {
  try {
    File->reopen(Filename);
  } catch (std::exception const &E) {
    Logger->error("Exception when reopening file: {}", E.what());
    throw;
  }
}

std::string FileWriterTask::jobID() const { return JobId; }

hdf5::node::Group FileWriterTask::hdfGroup() { return File->H5File.root(); }

bool FileWriterTask::swmrEnabled() const { return File->isSWMREnabled(); }

void FileWriterTask::setJobId(std::string const &Id) { JobId = Id; }

std::string FileWriterTask::filename() const { return Filename; }

} // namespace FileWriter
