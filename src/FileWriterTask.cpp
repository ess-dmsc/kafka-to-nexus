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
} // namespace

std::atomic<uint32_t> n_FileWriterTask_created{0};

std::vector<DemuxTopic> &FileWriterTask::demuxers() { return Demuxers; }

/// Helper function for creating an ID.
///
/// \param ExtraValue Used to help make a unique ID.
/// \return A "unique" id.
uint64_t createId(int ExtraValue) {
  using namespace std::chrono;
  return (static_cast<uint64_t>(
              duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
                  .count())
          << 16) +
         (ExtraValue & 0xffff);
}

FileWriterTask::FileWriterTask(
    std::string const &ServiceID_,
    std::shared_ptr<KafkaW::ProducerTopic> StatusProducer_)
    : ServiceId(ServiceID_), StatusProducer(std::move(StatusProducer_)) {
  Id = createId(++n_FileWriterTask_created);
}

FileWriterTask::~FileWriterTask() {
  LOG(Sev::Debug, "~FileWriterTask");
  Demuxers.clear();
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

  // If source already exists then replace
  for (auto &Demux : Demuxers) {
    if (Demux.topic() == Source.topic()) {
      Demux.add_source(std::move(Source));
      return;
    }
  }

  // Add new source
  Demuxers.emplace_back(Source.topic());
  auto &Demux = Demuxers.back();
  Demux.add_source(std::move(Source));
}

void FileWriterTask::InitialiseHdf(std::string const &NexusStructure,
                                   std::string const &ConfigFile,
                                   std::vector<StreamHDFInfo> &HdfInfo,
                                   bool UseSwmr) {
  auto NexusStructureJson = hdf_parse(NexusStructure);
  auto ConfigFileJson = hdf_parse(ConfigFile);

  try {
    LOG(Sev::Info, "Creating HDF file {}", Filename);
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
    File.reopen(Filename, json::object());
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

hdf5::node::Group FileWriterTask::hdfGroup() { return File.H5File.root(); }

bool FileWriterTask::swmrEnabled() const { return File.isSWMREnabled(); }

void FileWriterTask::setJobId(std::string const &Id) { JobId = Id; }

json FileWriterTask::stats() const {
  auto Topics = json::object();
  for (auto &Demux : Demuxers) {
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
