#include "Source.h"
#include "MPIChild.h"
#include "helper.h"
#include "logger.h"
#include <chrono>
#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <thread>

#ifndef SOURCE_DO_PROCESS_MESSAGE
#define SOURCE_DO_PROCESS_MESSAGE 1
#endif

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
  rapidjson::Document jconf;
  {
    LOG(7, "config_file: {}", json_to_string(config_file));
    LOG(7, "command: {}", json_to_string(command));
    LOG(7, "config_stream: {}", json_to_string(config_stream));
    using namespace rapidjson;
    auto &a = jconf.GetAllocator();
    jconf.Parse(R""({"hdf":{},"shm":{"fname":"tmp-mmap"}})"");
    jconf["hdf"].AddMember("fname", command["file_attributes"]["file_name"], a);
    jconf.AddMember("stream", config_stream, a);
    jconf.AddMember("config_file", config_file, a);
    jconf.AddMember("queue_addr", Value().SetUint64(uint64_t(queue.get())), a);
    jconf.AddMember("cq_addr", Value().SetUint64(uint64_t(cq)), a);
    jconf.AddMember("log_level", Value(log_level), a);
    PrettyWriter<StringBuffer> wr(sbuf);
    jconf.Accept(wr);
    LOG(7, "config for mpi: {}", sbuf.GetString());
  }

  for (size_t i_child = 0; i_child < n_child; ++i_child) {
    auto child = MPIChild::ptr(new MPIChild);
    child->config =
        std::string(sbuf.GetString(), sbuf.GetString() + sbuf.GetSize() + 1);
    child->cmd = {bin.data(), bin.data() + bin.size() + 1};
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
#if USE_PARALLEL_WRITER
  bool do_mpi = true;
#else
  bool do_mpi = false;
#endif
  if (do_mpi) {
    // TODO yield on contention
    for (int i1 = 0; true; ++i1) {
      auto n = queue->push(msg);
      if (n == 0) {
        break;
      }
      if (i1 % (1 << 9) == (1 << 9) - 1) {
        LOG(3, "queue full  i1: {}  n: {}", i1, n);
      }
      if (i1 >= (1 << 12)) {
        LOG(3, "QUEUE IS FULL FOR TOO LONG TIME");
        break;
      }
      sleep_ms(4);
    }
    return ProcessMessageResult::OK();
  } else {
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
