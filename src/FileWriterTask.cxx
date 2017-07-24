#include "FileWriterTask.h"
#include "HDFFile.h"
#include "Source.h"
#include "logger.h"
#include <atomic>
#include <chrono>

namespace FileWriter {

using std::string;
using std::vector;

std::atomic<uint32_t> n_FileWriterTask_created{0};

class FileWriterTask_impl {
  friend class FileWriterTask;
  friend class ::Test___FileWriterTask___Create01;
  std::string hdf_filename;
  HDFFile hdf_file;
};

class SourceFactory_by_FileWriterTask {
private:
  Source create(string topic, string sourcename);
  friend class FileWriterTask;
};

Source SourceFactory_by_FileWriterTask::create(string topic,
                                               string sourcename) {
  return {topic, sourcename};
}

std::vector<DemuxTopic> &FileWriterTask::demuxers() { return _demuxers; }

FileWriterTask::FileWriterTask() {
  using namespace std::chrono;
  _id = duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
            .count();
  _id = (_id & uint64_t(-1) << 16) | (n_FileWriterTask_created & 0xffff);
  ++n_FileWriterTask_created;
  impl.reset(new FileWriterTask_impl);
}

FileWriterTask::~FileWriterTask() { LOG(9, "~FileWriterTask"); }

FileWriterTask &FileWriterTask::set_hdf_filename(std::string hdf_filename) {
  impl->hdf_filename = hdf_filename;
  return *this;
}

void FileWriterTask::add_source(Source &&source) {
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

int FileWriterTask::hdf_init(rapidjson::Value const &nexus_structure) {
  auto x = impl->hdf_file.init(impl->hdf_filename, nexus_structure);
  if (x) {
    return x;
  }
  for (auto &d : demuxers()) {
    for (auto &s : d.sources()) {
      s.second.hdf_init(impl->hdf_file);
    }
  }
  return 0;
}

void FileWriterTask::file_flush() {
  if (impl)
    impl->hdf_file.flush();
}

uint64_t FileWriterTask::id() const { return _id; }

} // namespace FileWriter
