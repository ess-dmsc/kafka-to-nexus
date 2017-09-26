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
  // auto & hdf_store = * hdf_store_placeholder;

  // not possible to use barriers because parallel HDF also uses barriers.
  LOG(3, ".....................  wait for  CLOSING");
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
  cq->barriers[0]++;
  cq->wait_for_barrier(hdf_store, 0, 0);
  // LOG(3, "sleep before closing");
  // sleep_ms(6000);
  LOG(3, "===============================  CLOSING   "
         "=========================================");
  // sleep_ms(600);

  LOG(3, ".....................  wait for  CQ EXEC");
  /*
  for (int i1 = 0; i1 < size - 1; ++i1) {
    MPI_Recv(&payload, sizeof(payload), MPI_CHAR, MPI_ANY_SOURCE, 15, comm_all,
  &status);
    LOG(3, "recv a 13");
  }
  for (int i1 = 0; i1 < size - 1; ++i1) {
    MPI_Send(&payload, sizeof(payload), MPI_CHAR, 1 + i1, 16, comm_all);
    LOG(3, "send a 14 to {}", 1 + i1);
  }
  */
  cq->barriers[1]++;
  cq->wait_for_barrier(hdf_store, 1, 0);
  // LOG(3, "sleep before exec");
  // sleep_ms(6000);
  LOG(3, "===============================  CQ EXEC   "
         "=========================================");
  // sleep_ms(600);

  // cq->execute_for(hdf_store, 0);

  LOG(6, ".....................  wait for  CQ EXEC 2");
  cq->barriers[2]++;
  cq->wait_for_barrier(hdf_store, 2, 0);
  LOG(6, "===============================  CQ EXEC 2   "
         "=======================================");

  // cq->execute_for(hdf_store, 1);

  LOG(6, ".....................  wait for  MPI BARRIER");
  cq->barriers[3]++;
  cq->wait_for_barrier(hdf_store, 3, 1);
  LOG(6, "===============================  MPI BARRIER   "
         "=====================================");

  LOG(3, "Barrier 2 BEFORE");
  err = MPI_Barrier(comm_all);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Barrier");
    exit(1);
  }
  LOG(3, "Barrier 2 AFTER");
  LOG(3, "ask for disconnect");
  err = MPI_Comm_disconnect(&comm_all);
  // err = MPI_Comm_disconnect(&comm_spawned);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
    exit(1);
  }
  LOG(6, ".....................  wait for  LAST BARRIER");
  cq->barriers[4]++;
  cq->wait_for_barrier(hdf_store, 4, -1);
  // LOG(3, "sleep after disconnect");
  // sleep_ms(2000);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
    exit(1);
  }
  // MPI_Finalize();
}

} // namespace FileWriter
