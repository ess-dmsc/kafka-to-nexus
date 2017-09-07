#include "Source.h"
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
               Jemalloc::sptr jm, MMap::sptr mmap)
    : _sourcename(sourcename), _hdf_writer_module(std::move(hdf_writer_module)),
      jm(jm), mmap(mmap) {
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
}

std::string const &Source::topic() const { return _topic; }

std::string const &Source::sourcename() const { return _sourcename; }

void Source::mpi_start(rapidjson::Document config_file,
                       rapidjson::Document command,
                       rapidjson::Document config_stream) {
  LOG(3, "Source::mpi_start()");
  jm->use_this();
  char *x;

  while (true) {
    x = (char *)malloc(sizeof(char));
    // x = (char*)jm->alloc(8 * 1024 * sizeof(char));
    if (jm->check_in_range(x))
      break;
  }

  while (true) {
    x = (char *)new MsgQueue;
    if (jm->check_in_range(x))
      break;
  }

  for (int i1 = 0; i1 < 0; ++i1) {
    LOG(3, "alloc chunk {}", i1);
    x = (char *)malloc(10 * 1024 * 1024 * sizeof(char));
    // x = (char*)jm->alloc(8 * 1024 * sizeof(char));
    if (not jm->check_in_range(x)) {
      LOG(3, "fail check_in_range");
      exit(1);
    }
  }

  // x = (char*)malloc(32 * 1024 * sizeof(char));
  // x = (char*)jm->alloc(32 * 1024 * sizeof(char));
  // jm->check_in_range(x);

  // jm->use_default();

  // jm->use_this();
  queue = MsgQueue::ptr(new MsgQueue);
  if (not jm->check_in_range(queue.get())) {
    LOG(3, "mem error");
    exit(1);
  }
  jm->use_default();

  rapidjson::StringBuffer sbuf;
  {
    LOG(3, "config_file: {}", json_to_string(config_file));
    LOG(3, "command: {}", json_to_string(command));
    LOG(3, "config_stream: {}", json_to_string(config_stream));
    using namespace rapidjson;
    Document jconf;
    jconf.Parse(R""({"hdf":{},"shm":{"fname":"tmp-mmap"}})"");
    jconf["hdf"].AddMember("fname", command["file_attributes"]["file_name"],
                           jconf.GetAllocator());
    jconf.AddMember("stream", config_stream, jconf.GetAllocator());
    jconf.AddMember("config_file", config_file, jconf.GetAllocator());
    Writer<StringBuffer> wr(sbuf);
    jconf.Accept(wr);
    // LOG(3, "config for mpi: {}", sbuf.GetString());
  }
  int err = MPI_SUCCESS;
  MPI_Info mpi_info;
  if (MPI_Info_create(&mpi_info) != MPI_SUCCESS) {
    LOG(3, "ERROR can not init MPI_Info");
    exit(1);
  }
  char arg1[32];
  strcpy(arg1, "--mpi");
  char *argv[] = {
      arg1, (char *)sbuf.GetString(), nullptr,
  };
  err = MPI_Comm_spawn("./mpi-worker", argv, nspawns, MPI_INFO_NULL, 0,
                       MPI_COMM_WORLD, &comm_spawned, mpi_return_codes.data());
  if (err != MPI_SUCCESS) {
    LOG(3, "can not spawn");
    exit(1);
  }
  {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    LOG(3, "After spawn in main MPI_COMM_WORLD  rank: {}  size: {}", rank,
        size);
  }
  {
    int rank, size;
    MPI_Comm_rank(comm_spawned, &rank);
    MPI_Comm_size(comm_spawned, &size);
    LOG(3, "comm_spawned  rank: {}  size: {}", rank, size);
  }
  err = MPI_Intercomm_merge(comm_spawned, 0, &comm_all);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Intercomm_merge");
    exit(1);
  }
  {
    int rank, size;
    MPI_Comm_rank(comm_all, &rank);
    MPI_Comm_size(comm_all, &size);
    LOG(3, "comm_all rank: {}  size: {}", rank, size);
  }
  LOG(3, "Barrier 1 BEFORE");
  MPI_Barrier(comm_all);
  LOG(3, "Barrier 1 AFTER");
  // exchange config
  // wait for their ok that they opened file
}

void Source::mpi_stop() {
  LOG(3, "Source::mpi_stop()");
  // send stop command, wait for group size zero?
  int err = MPI_SUCCESS;
  LOG(3, "Barrier 2 BEFORE");
  MPI_Barrier(comm_all);
  LOG(3, "Barrier 2 AFTER");
  LOG(3, "ask for disconnect");
  err = MPI_Comm_disconnect(&comm_all);
  // err = MPI_Comm_disconnect(&comm_spawned);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
    exit(1);
  }
  LOG(3, "sleeping");
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // MPI_Finalize();
}

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
    queue->push(std::move(msg));
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
