#include "FileWriterTask.h"
#include "HDFFile.h"
#include "Source.h"
#include "helper.h"
#include "logger.h"
#include <atomic>
#include <chrono>
#include <thread>

namespace FileWriter {

using std::string;
using std::vector;

std::atomic<uint32_t> n_FileWriterTask_created{0};

std::vector<DemuxTopic> &FileWriterTask::demuxers() { return _demuxers; }

FileWriterTask::FileWriterTask() {
  using namespace std::chrono;
  _id = static_cast<uint64_t>(
      duration_cast<nanoseconds>(system_clock::now().time_since_epoch())
          .count());
  _id = (_id & uint64_t(-1) << 16) | (n_FileWriterTask_created & 0xffff);
  ++n_FileWriterTask_created;
}

FileWriterTask::~FileWriterTask() {
  LOG(6, "~FileWriterTask");
  mpi_stop();
  _demuxers.clear();
}

FileWriterTask &FileWriterTask::set_hdf_filename(std::string hdf_filename) {
  this->hdf_filename = hdf_filename;
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

int FileWriterTask::hdf_init(rapidjson::Value const &nexus_structure,
                             rapidjson::Value const &config_file,
                             std::vector<StreamHDFInfo> &stream_hdf_info,
                             std::vector<hid_t> &groups) {
  auto x = hdf_file.init(hdf_filename, nexus_structure, config_file,
                         stream_hdf_info, groups);
  if (x) {
    LOG(3, "can not initialize hdf file  filename: {}", hdf_filename);
    return x;
  }
  return 0;
}

uint64_t FileWriterTask::id() const { return _id; }
std::string FileWriterTask::job_id() const { return _job_id; }

void FileWriterTask::job_id_init(const std::string &s) { _job_id = s; }

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
  js_fwt.AddMember("filename", Value(hdf_filename.c_str(), a), a);
  js_fwt.AddMember("topics", js_topics, a);
  return js_fwt;
}

void FileWriterTask::mpi_start(std::vector<MPIChild::ptr> &&to_spawn) {
  // Have to participate also in collective:
  hdf_file.cq->open();

  int err = MPI_SUCCESS;
  MPI_Info mpi_info;
  if (MPI_Info_create(&mpi_info) != MPI_SUCCESS) {
    LOG(3, "ERROR can not init MPI_Info");
    exit(1);
  }

  vector<char *> cmd_m;
  vector<char **> argv_m;
  vector<int> maxprocs_m;
  vector<MPI_Info> mpi_infos_m;
  MPI_Comm comm_spawned;
  vector<int> proc_err_m;
  for (auto &x : to_spawn) {
    cmd_m.push_back(x->cmd.data());
    argv_m.push_back(x->argv.data());
    maxprocs_m.push_back(1);
    mpi_infos_m.push_back(MPI_INFO_NULL);
    proc_err_m.push_back(0);
  }
  LOG(3, "spawning  n: {}", cmd_m.size());
  err = MPI_Comm_spawn_multiple(
      cmd_m.size(), cmd_m.data(), argv_m.data(), maxprocs_m.data(),
      mpi_infos_m.data(), 0, MPI_COMM_WORLD, &comm_spawned, proc_err_m.data());
  if (err != MPI_SUCCESS) {
    LOG(3, "can not spawn");
    exit(1);
  }
  {
    int flag = 0;
    err = MPI_Comm_test_inter(comm_spawned, &flag);
    LOG(3, "MPI_Comm_test_inter comm_spawned: {}", flag);
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
  int comm_all_size = 0;
  {
    int rank, size;
    MPI_Comm_rank(comm_all, &rank);
    MPI_Comm_size(comm_all, &size);
    LOG(3, "comm_all rank: {}  size: {}", rank, size);
    comm_all_size = size;
  }

  if (sizeof(MPI_Win) != 4) {
    LOG(3, "ERROR sizeof(MPI_Win)");
    exit(1);
  }

  bool participate_shm = true;
  auto split_comm_shm_from = comm_all;

  MPI_Comm comm_shm;
  int comm_shm_rank = -1;

  {
    int flag = 0;
    err = MPI_Comm_test_inter(comm_all, &flag);
    if (err != MPI_SUCCESS) {
      LOG(3, "error");
      exit(1);
    }
    LOG(3, "MPI_Comm_test_inter comm_all: {}", flag);
    if (flag != 0) {
      LOG(3, "ERROR expect comm_all to be intra-comm");
      exit(1);
    }
  }

  if (participate_shm) {
    LOG(3, "splitting shm");
    err = MPI_Comm_split_type(split_comm_shm_from, MPI_COMM_TYPE_SHARED, 0,
                              MPI_INFO_NULL, &comm_shm);
    if (err != MPI_SUCCESS) {
      LOG(3, "err MPI_Comm_split_type");
      exit(1);
    }
    {
      int flag = 0;
      err = MPI_Comm_test_inter(comm_shm, &flag);
      if (err != MPI_SUCCESS) {
        LOG(3, "error");
        exit(1);
      }
      LOG(3, "MPI_Comm_test_inter comm_shm: {}", flag);
      if (flag != 0) {
        LOG(3, "ERROR expect comm_shm to be intra-comm");
        exit(1);
      }
    }
    {
      int rank, size;
      MPI_Comm_rank(comm_shm, &rank);
      MPI_Comm_size(comm_shm, &size);
      LOG(3, "comm_shm rank: {}  size: {}", rank, size);
      comm_shm_rank = rank;
    }
  }

  void *baseptr = nullptr;
  MPI_Win win;

  if (participate_shm) {
    // Allocate
    // MPI_Info info2;
    // MPI_Info_create(&info2);
    err = MPI_Win_allocate_shared(1024 * 1024, 1, MPI_INFO_NULL, comm_shm,
                                  &baseptr, &win);
    if (err != MPI_SUCCESS) {
      LOG(3, "can not MPI_Win_allocate_shared");
    }
    LOG(3, "baseptr: {}", baseptr);
  }

  if (false) {
    // Lock
    err = MPI_Win_lock_all(MPI_MODE_NOCHECK, win);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Win_lock_all");
      exit(1);
    }
  }

  if (participate_shm) {
    // Write memory
    auto s1 = fmt::format("proc-{}", comm_shm_rank);
    s1.push_back(0);
    memcpy(baseptr, s1.data(), s1.size());
  }

  if (false) {
    for (int dest = 1; dest < comm_all_size; ++dest) {
      MPI_Send(&win, 1, MPI_INT, dest, 101, comm_all);
    }
  }

  if (participate_shm) {
    // Fence
    err = MPI_Win_fence(0, win);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Win_fence");
      exit(1);
    }

    LOG(3, "Barrier comm_shm 1 BEFORE");
    MPI_Barrier(comm_shm);
    LOG(3, "Barrier comm_shm 1 AFTER");

    // Fence
    err = MPI_Win_fence(0, win);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Win_fence");
      exit(1);
    }
  }

  LOG(3, "Barrier 1 BEFORE");
  MPI_Barrier(comm_all);
  LOG(3, "Barrier 1 AFTER");
}

void FileWriterTask::mpi_stop() {
  LOG(3, "FileWriterTask::mpi_stop()");

  int rank, size;
  {
    MPI_Comm_rank(comm_all, &rank);
    MPI_Comm_size(comm_all, &size);
    LOG(3, "comm_all rank: {}  size: {}", rank, size);
  }

  // send stop command, wait for group size zero?
  for (auto &d : _demuxers) {
    for (auto &s : d.sources()) {
      s.second.queue->open.store(0);
    }
  }

  int err = MPI_SUCCESS;

  auto &cq = hdf_file.cq;

  HDFIDStore *hdf_store = nullptr;
  int cqid = -1;

  auto barrier = [&cq, &cqid, &hdf_store](size_t id, size_t queue,
                                          std::string name) {
    LOG(6, "...............................  cqid: {}  wait   {}  {}", cqid, id,
        name);
    cq->barriers[id]++;
    cq->wait_for_barrier(hdf_store, id, queue);
    LOG(6, "===============================  cqid: {}  after  {}  {}", cqid, id,
        name);
  };

  barrier(0, 0, "MODULE RESET");
  /*
  MPI_Status status;
  size_t payload = 0;
  for (int i1 = 0; i1 < size - 1; ++i1) {
    MPI_Recv(&payload, sizeof(payload), MPI_CHAR, MPI_ANY_SOURCE, 13, comm_all,
  &status);
    LOG(3, "recv a 13");
  }
  for (int i1 = 0; i1 < size - 1; ++i1) {
    MPI_Send(&payload, sizeof(payload), MPI_CHAR, 1 + i1, 14, comm_all);
    LOG(3, "send a 14 to {}", 1 + i1);
  }
  */
  // sleep_ms(600);

  barrier(1, 0, "CQ EXEC");
  // cq->execute_for(hdf_store, 0);

  barrier(2, 0, "CQ EXEC 2");
  // cq->execute_for(hdf_store, 1);

  barrier(5, 1, "CQ EXEC 3");
  // cq->execute_for(hdf_store, 2);

  barrier(3, 2, "MPI Barrier");
  err = MPI_Barrier(comm_all);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Barrier");
    exit(1);
  }
  LOG(6, "ask for disconnect  cqid: {}", "main");
  err = MPI_Comm_disconnect(&comm_all);
  // err = MPI_Comm_disconnect(&comm_spawned);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
    exit(1);
  }
  barrier(4, -1, "Last CQ barrier");
  // LOG(3, "sleep after disconnect");
  // sleep_ms(2000);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
    exit(1);
  }
  // MPI_Finalize();
}

} // namespace FileWriter
