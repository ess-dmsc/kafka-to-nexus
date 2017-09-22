#include "Source.h"
#include "MPIChild.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <thread>

#ifndef SOURCE_DO_PROCESS_MESSAGE
#define SOURCE_DO_PROCESS_MESSAGE 1
#endif

/*
void * operator new(std::size_t n) throw(std::bad_alloc) {
}
void operator delete(void * p) throw() {
}
*/

namespace FileWriter {

Result Result::Ok() {
  Result ret;
  ret._res = 0;
  return ret;
}

Source::Source(std::string sourcename, HDFWriterModule::ptr hdf_writer_module,
               Jemalloc::sptr jm, MMap::sptr mmap, CollectiveQueue *cq)
    : _sourcename(sourcename), _hdf_writer_module(std::move(hdf_writer_module)),
      jm(jm), mmap(mmap), cq(cq) {
  if (SOURCE_DO_PROCESS_MESSAGE == 0) {
    do_process_message = false;
  }
}

Source::Source(Source &&x) noexcept { swap(*this, x); }

void swap(Source &x, Source &y) {
  using std::swap;
  swap(x._topic, y._topic);
  swap(x._sourcename, y._sourcename);
  swap(x._hdf_writer_module, y._hdf_writer_module);
  swap(x._processed_messages_count, y._processed_messages_count);
  swap(x._cnt_msg_written, y._cnt_msg_written);
  swap(x.do_process_message, y.do_process_message);
  swap(x.jm, y.jm);
  swap(x.mmap, y.mmap);
  swap(x.queue, y.queue);
  swap(x.comm_spawned, y.comm_spawned);
  swap(x.comm_all, y.comm_all);
  swap(x.nspawns, y.nspawns);
  swap(x.mpi_return_codes, y.mpi_return_codes);
  swap(x.cq, y.cq);
}

std::string const &Source::topic() const { return _topic; }

std::string const &Source::sourcename() const { return _sourcename; }

void Source::mpi_start(rapidjson::Document config_file,
                       rapidjson::Document command,
                       rapidjson::Document config_stream,
                       std::vector<MPIChild::ptr> &spawns) {
  LOG(3, "Source::mpi_start()");
  jm->use_this();
  queue = MsgQueue::ptr(new MsgQueue);
  if (not jm->check_in_range(queue.get())) {
    LOG(3, "mem error");
    exit(1);
  }
  jm->use_default();

  auto bin =
      fmt::format("{}/mpi-worker", config_file["mpi"]["path_bin"].GetString());

  int n_child = 1;
  if (auto x = get_int(&config_stream, "n_mpi_workers")) {
    n_child = x.v;
  }

  LOG(3, "make jconf");
  rapidjson::StringBuffer sbuf;
  {
    LOG(7, "config_file: {}", json_to_string(config_file));
    LOG(7, "command: {}", json_to_string(command));
    LOG(7, "config_stream: {}", json_to_string(config_stream));
    using namespace rapidjson;
    Document jconf;
    jconf.Parse(R""({"hdf":{},"shm":{"fname":"tmp-mmap"}})"");
    jconf["hdf"].AddMember("fname", command["file_attributes"]["file_name"],
                           jconf.GetAllocator());
    jconf.AddMember("stream", config_stream, jconf.GetAllocator());
    jconf.AddMember("config_file", config_file, jconf.GetAllocator());
    jconf.AddMember("queue_addr", Value().SetUint64(uint64_t(queue.get())),
                    jconf.GetAllocator());
    jconf.AddMember("cq_addr", Value().SetUint64(uint64_t(cq)),
                    jconf.GetAllocator());
    Writer<StringBuffer> wr(sbuf);
    jconf.Accept(wr);
    LOG(7, "config for mpi: {}", sbuf.GetString());
  }

  for (size_t i_child = 0; i_child < n_child; ++i_child) {
    auto child = MPIChild::ptr(new MPIChild);
    child->cmd = {bin.data(), bin.data() + bin.size() + 1};
    child->args.push_back(
        {sbuf.GetString(), sbuf.GetString() + sbuf.GetSize() + 1});
    char const *s;
    s = "--mpi";
    child->args.push_back({s, s + strlen(s) + 1});
    s = "-vvvv";
    child->args.push_back({s, s + strlen(s) + 1});
    for (auto &x : child->args) {
      child->argv.push_back(x.data());
    }
    child->argv.push_back(nullptr);
    spawns.push_back(std::move(child));
  }
}

void Source::mpi_stop() { LOG(3, "mpi_stop()  nothing to do"); }

ProcessMessageResult Source::process_message(Msg &msg) {
  auto &reader = FlatbufferReaderRegistry::find(msg);
  if (!reader->verify(msg)) {
    LOG(5, "buffer not verified");
    return ProcessMessageResult::ERR();
  }
  if (!do_process_message) {
    return ProcessMessageResult::OK();
  }
  bool do_mpi = true;
  if (do_mpi) {
    // TODO yield on contention
    for (int i1 = 0; true; ++i1) {
      if (queue->push(msg) == 0) {
        break;
      }
      if (i1 >= 10000) {
        LOG(3, "QUEUE IS FULL FOR TOO LONG TIME");
        break;
      }
      sleep_ms(1);
    }
    return ProcessMessageResult::OK();
  }
  if (!_hdf_writer_module) {
    throw "ASSERT FAIL: _hdf_writer_module";
  }
  auto ret = _hdf_writer_module->write(msg);
  _cnt_msg_written += 1;
  _processed_messages_count += 1;
  if (ret.is_ERR()) {
    return ProcessMessageResult::ERR();
  }
  if (ret.is_OK_WITH_TIMESTAMP()) {
    return ProcessMessageResult::OK(ret.timestamp());
  }
  return ProcessMessageResult::OK();
}

uint64_t Source::processed_messages_count() const {
  return _processed_messages_count;
}

std::string Source::to_str() const { return json_to_string(to_json()); }

rapidjson::Document
Source::to_json(rapidjson::MemoryPoolAllocator<> *_a) const {
  using namespace rapidjson;
  Document jd;
  if (_a)
    jd = Document(_a);
  auto &a = jd.GetAllocator();
  jd.SetObject();
  auto &v = jd;
  v.AddMember("__KLASS__", "Source", a);
  v.AddMember("topic", Value().SetString(topic().data(), a), a);
  v.AddMember("source", Value().SetString(sourcename().data(), a), a);
  return jd;
}

} // namespace FileWriter
