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

std::vector<DemuxTopic> &FileWriterTask::demuxers() { return _demuxers; }

FileWriterTask::FileWriterTask(
    std::string ServiceID_,
    std::shared_ptr<KafkaW::ProducerTopic> StatusProducer_)
    : ServiceID(ServiceID_), StatusProducer(StatusProducer_) {
  using namespace std::chrono;
  _id = static_cast<uint64_t>(
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
          .count());
  _id = (_id & uint64_t(-1) << 16) | (n_FileWriterTask_created & 0xffff);
  ++n_FileWriterTask_created;
}

FileWriterTask::~FileWriterTask() {
  LOG(Sev::Debug, "~FileWriterTask");
  _demuxers.clear();
  try {
    hdf_file.close();
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Close, ServiceID, JobID,
               "File closed");
    }
  } catch (std::exception const &E) {
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Fail, ServiceID, JobID,
               fmt::format("Exception while finishing FileWriterTask: {}",
                           E.what()));
    }
  }
}

FileWriterTask &FileWriterTask::set_hdf_filename(std::string hdf_output_prefix,
                                                 std::string hdf_filename) {
  this->hdf_output_prefix = hdf_output_prefix;
  this->hdf_filename = hdf_filename;
  return *this;
}

void FileWriterTask::add_source(Source &&source) {
  if (UseHDFSWMR) {
    source.HDFFileForSWMR = &hdf_file;
  }
  bool found = false;
  for (auto &d : _demuxers) {
    if (d.topic() == source.topic()) {
      d.add_source(std::move(source));
      found = true;
    }
  }
  if (!found) {
    _demuxers.emplace_back(source.topic());
    auto &d = _demuxers.back();
    d.add_source(std::move(source));
  }
}

/// Called by CommandHandler on setup.
void FileWriterTask::hdf_init(std::string const &NexusStructure,
                              std::string const &ConfigFile,
                              std::vector<StreamHDFInfo> &stream_hdf_info) {
  filename_full = hdf_filename;
  if (!hdf_output_prefix.empty()) {
    filename_full = hdf_output_prefix + "/" + filename_full;
  }

  auto NexusStructureJson = hdf_parse(NexusStructure);
  auto ConfigFileJson = hdf_parse(ConfigFile);

  try {
    LOG(Sev::Info, "Creating HDF file {}", filename_full);
    hdf_file.init(filename_full, NexusStructureJson, ConfigFileJson,
                  stream_hdf_info, UseHDFSWMR);
  } catch (...) {
    std::throw_with_nested(std::runtime_error(fmt::format(
        "can not initialize hdf file  hdf_output_prefix: {}  hdf_filename: {}",
        hdf_output_prefix, hdf_filename)));
  }
}

void FileWriterTask::hdf_close_before_reopen() { hdf_file.close(); }

int FileWriterTask::hdf_reopen() {
  try {
    hdf_file.reopen(filename_full, json::object());
  } catch (std::exception const &E) {
    LOG(Sev::Error, "Exception: {}", E.what());
    if (StatusProducer) {
      logEvent(StatusProducer, StatusCode::Error, ServiceID, JobID,
               fmt::format("Exception: {}", E.what()));
    }
    return -1;
  }
  return 0;
}

uint64_t FileWriterTask::id() const { return _id; }

std::string FileWriterTask::job_id() const { return JobID; }

/// \brief  Set our `JobID` to the passed value.
void FileWriterTask::job_id_init(std::string const &JobID_) { JobID = JobID_; }

json FileWriterTask::stats() const {
  auto Topics = json::object();
  for (auto &d : _demuxers) {
    auto Demux = json::object();
    Demux["messages_processed"] = d.messages_processed.load();
    Demux["error_message_too_small"] = d.error_message_too_small.load();
    Demux["error_no_flatbuffer_reader"] = d.error_no_flatbuffer_reader.load();
    Demux["error_no_source_instance"] = d.error_no_source_instance.load();
    Topics[d.topic()] = Demux;
  }
  auto FWT = json::object();
  FWT["filename"] = hdf_filename;
  FWT["topics"] = Topics;
  return FWT;
}

} // namespace FileWriter
