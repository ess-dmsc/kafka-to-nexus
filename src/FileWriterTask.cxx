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
  _id = static_cast<uint64_t>(
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
          .count());
  _id = (_id & uint64_t(-1) << 16) | (n_FileWriterTask_created & 0xffff);
  ++n_FileWriterTask_created;
  impl.reset(new FileWriterTask_impl);
}

FileWriterTask::~FileWriterTask() { LOG(6, "~FileWriterTask"); }

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
  std::vector<StreamHDFInfo> stream_hdf_info;
  auto x =
      impl->hdf_file.init(impl->hdf_filename, nexus_structure, stream_hdf_info);
  if (x) {
    return x;
  }
  return 0;
}

uint64_t FileWriterTask::id() const { return _id; }

rapidjson::Value FileWriterTask::stats(
    rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> &a) const {
  using namespace rapidjson;
  Value js_topics;
  js_topics.SetObject();
  for (auto &d : _demuxers) {
    js_topics.AddMember(Value(d.topic().c_str(), a), Value(0), a);
  }
  Value js_fwt;
  js_fwt.SetObject();
  js_fwt.AddMember("filename", Value(impl->hdf_filename.c_str(), a), a);
  js_fwt.AddMember("topics", js_topics, a);
  return js_fwt;
}

} // namespace FileWriter
