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
    for (int target = 0; target < cmd_m.size(); ++target) {
      auto &child = to_spawn.at(target);
      MPI_Send(child->config.data(), child->config.size(), MPI_CHAR, target,
               101, comm_spawned);
    }
  }

  err = MPI_Intercomm_merge(comm_spawned, 0, &comm_all);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Intercomm_merge");
    exit(1);
  }

  int comm_all_size = -1;
  int comm_all_rank = -1;
  {
    int rank, size;
    MPI_Comm_rank(comm_all, &rank);
    MPI_Comm_size(comm_all, &size);
    LOG(3, "comm_all rank: {}  size: {}", rank, size);
    comm_all_size = size;
    comm_all_rank = rank;
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
