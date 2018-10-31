#include "FileWriterTask.h"
#include "EventLogger.h"
#include "HDFFile.h"
#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <atomic>
#include <chrono>
#include <thread>

namespace FileWriter {

namespace {

using std::string;
using std::vector;
using nlohmann::json;

json hdf_parse(std::string const &Structure) {
  try {
    auto StructureDocument = json::parse(Structure);
    return StructureDocument;
  } catch (...) {
    LOG(Sev::Error, "Parse Error: ", Structure)
    throw FileWriter::ParseError(Structure);
  }
}
}

std::atomic<uint32_t> n_FileWriterTask_created{0};

std::vector<DemuxTopic> &FileWriterTask::demuxers() { return Demuxers; }

FileWriterTask::FileWriterTask(
    std::string ServiceID_,
    std::shared_ptr<KafkaW::ProducerTopic> StatusProducer_)
    : ServiceId(ServiceID_), StatusProducer(std::move(StatusProducer_)) {
  using namespace std::chrono;
  Id = static_cast<uint64_t>(
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
          .count());
  Id = (Id & uint64_t(-1) << 16) | (n_FileWriterTask_created & 0xffff);
  ++n_FileWriterTask_created;
}

FileWriterTask::~FileWriterTask() {
  LOG(Sev::Debug, "~FileWriterTask");
  Demuxers.clear();
  try {
    hdfFile.close();
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
    Source.HDFFileForSWMR = &hdfFile;
  }

  // If source already exists then replace
  for (auto &d : Demuxers) {
    if (d.topic() == Source.topic()) {
      d.add_source(std::move(Source));
      return;
    }
  }

  // Add new source
  Demuxers.emplace_back(Source.topic());
  auto &d = Demuxers.back();
  d.add_source(std::move(Source));
}

void FileWriterTask::InitialiseHdf(std::string const &NexusStructure,
                                   std::string const &ConfigFile,
                                   std::vector<StreamHDFInfo> &HdfInfo,
                                   bool UseSwmr) {
  auto NexusStructureJson = hdf_parse(NexusStructure);
  auto ConfigFileJson = hdf_parse(ConfigFile);

  try {
    LOG(Sev::Info, "Creating HDF file {}", Filename);
    hdfFile.init(Filename, NexusStructureJson, ConfigFileJson,
                  HdfInfo, UseSwmr);
    // The HDF file is closed and re-opened to (optionally) support SWMR and
    // parallel writing.
    closeFile();
    reopenFile();

  } catch (std::exception const &E) {
    std::throw_with_nested(std::runtime_error(fmt::format(
        "can not initialize hdf file {}", Filename)));
  }
}

void FileWriterTask::closeFile() { hdfFile.close(); }

void FileWriterTask::reopenFile() {
  try {
    hdfFile.reopen(Filename, json::object());
  } catch (std::exception const &E) {
    LOG(Sev::Error, "Exception: {}", E.what());
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Error, ServiceId, JobId,
               fmt::format("Exception: {}", E.what()));
    }
    throw;
  }
}

uint64_t FileWriterTask::id() const { return Id; }

std::string FileWriterTask::jobID() const { return JobId; }

hdf5::node::Group FileWriterTask::hdfGroup() {
  return hdfFile.H5File.root();
}

bool FileWriterTask::swmrEnabled() const {
  return hdfFile.isSWMREnabled();
}

void FileWriterTask::setJobId(std::string const &Id) { JobId = Id; }

json FileWriterTask::stats() const {
  auto Topics = json::object();
  for (auto &d : Demuxers) {
    auto Demux = json::object();
    Demux["messages_processed"] = d.messages_processed.load();
    Demux["error_message_too_small"] = d.error_message_too_small.load();
    Demux["error_no_flatbuffer_reader"] = d.error_no_flatbuffer_reader.load();
    Demux["error_no_source_instance"] = d.error_no_source_instance.load();
    Topics[d.topic()] = Demux;
  }
  auto FWT = json::object();
  FWT["filename"] = Filename;
  FWT["topics"] = Topics;
  return FWT;
}
std::string FileWriterTask::filename() const {
  return Filename;
}

} // namespace FileWriter
