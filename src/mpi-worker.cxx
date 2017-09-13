#include "MMap.h"
#include "logger.h"
#include <atomic>
#include <chrono>
#include <mpi.h>
#include <thread>
#include <vector>

// getpid()
#include <sys/types.h>
#include <unistd.h>

#include <sys/mman.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "CollectiveQueue.h"
#include "HDFFile.h"
#include "HDFWriterModule.h"
#include "MsgQueue.h"
#include "helper.h"
#include "json.h"
#include "logpid.h"

using std::array;
using std::vector;
using std::string;

using CLK = std::chrono::steady_clock;
using MS = std::chrono::milliseconds;

void older(MPI_Comm comm_parent, MPI_Comm comm_all) {
  std::vector<char> buf(128);
  int rank, size;
  int err;
  // MPI_STATUS_IGNORE
  MPI_Status status;
  MPI_Recv(buf.data(), buf.size(), MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG,
           comm_parent, &status);
  int count;
  MPI_Get_count(&status, MPI_CHAR, &count);
  LOG(3, "status: {}, {}, {}, count: {}", status.MPI_SOURCE, status.MPI_TAG,
      status.MPI_ERROR, count);
  LOG(3, "received: {}", buf.data());

  LOG(3, "wait for pointer");
  void *shm_ptr = nullptr;
  MPI_Recv(&shm_ptr, 8, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, comm_parent,
           &status);
  // LOG(3, "read shared data: {}", shm_ptr);
  LOG(3, "recv shm_ptr: {}", (void *)shm_ptr);

  auto &shm_comm = comm_all;
  int shm_rank = -1;
  MPI_Comm_rank(shm_comm, &rank);
  MPI_Comm_size(shm_comm, &size);
  LOG(3, "mpi-worker in shm_comm rank: {}  size: {}", rank, size);
  shm_rank = rank;

  // TODO
  // Share the setup code with the main process
  size_t shm_size = 0 * 1024 * 1024;
  MPI_Win mpi_win;
  MPI_Info win_info;
  MPI_Info_create(&win_info);
  MPI_Info_set(win_info, "alloc_shared_noncontig", "true");
  LOG(3, "MPI_Win_allocate_shared {}", rank);
  err = MPI_Win_allocate_shared(shm_size, 1, win_info, shm_comm, &shm_ptr,
                                &mpi_win);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Win_allocate_shared");
    exit(1);
  }
  LOG(3, "MPI_Win_allocate_shared {} DONE", rank);

  {
    MPI_Group g;
    err = MPI_Win_get_group(mpi_win, &g);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Win_get_group");
      exit(1);
    }
    int rank = -1;
    err = MPI_Group_rank(g, &rank);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Group_rank");
      exit(1);
    }
    LOG(3, "our rank in mpi_win is: {}", rank);
  }

  MPI_Aint shm_size_q = 0;
  int disp_unit = 0;
  err = MPI_Win_shared_query(mpi_win, 1, &shm_size_q, &disp_unit, &shm_ptr);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Win_shared_query");
    exit(1);
  }
  LOG(3, "queried shm_ptr: {:p}", shm_ptr);
  if (shm_size_q != shm_size) {
    LOG(3, "shm_size_q != shm_size : {} != {}", shm_size_q, shm_size);
    // exit(1);
  }
  LOG(3, "MPI_Win_shared_query DONE  disp_unit: {}", disp_unit);

  MPI_Win_lock_all(0, mpi_win);
  MPI_Win_sync(mpi_win);
  MPI_Barrier(shm_comm);
  LOG(3, "after barrier");

  FILE *f1 = fopen("tmp-pid2.txt", "wb");
  auto pidstr = fmt::format("{}", getpid());
  fwrite(pidstr.data(), pidstr.size(), 1, f1);
  fclose(f1);
  // std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  if (false) {
    auto m1 = (std::atomic<uint32_t> *)shm_ptr;
    while (m1->load() != 1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      LOG(3, "value: {}", m1->load());
    }
    LOG(3, "value: {}", m1->load());
  }

  MPI_Barrier(shm_comm);
  LOG(3, "after barrier 2");

  MPI_Info_free(&win_info);
}

int main(int argc, char **argv) {
  log_level = 7;

  if (argc < 3) {
    LOG(3, "not enough arguments");
    return -1;
  }
  // LOG(3, "conf: {}", argv[2]);
  using namespace rapidjson;
  Document jconf;
  jconf.Parse(argv[1]);
  LOG(3, "jconf: {}", json_to_string(jconf));

  int err = MPI_SUCCESS;
  MPI_Init(&argc, &argv);

  MPI_Comm MPI_COMM_NODE;
  MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0 /* key */,
                      MPI_INFO_NULL, &MPI_COMM_NODE);

  int rank_world, size_world;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank_world);
  MPI_Comm_size(MPI_COMM_WORLD, &size_world);
  LOG(3, "mpi-worker  rank_world: {}  size_world: {}", rank_world, size_world);
  MPI_Comm comm_parent;
  MPI_Comm_get_parent(&comm_parent);

  int rank_merged, size_merged;
  MPI_Comm comm_all;
  {
    err = MPI_Intercomm_merge(comm_parent, 1, &comm_all);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Intercomm_merge");
      exit(1);
    }
    MPI_Comm_rank(comm_all, &rank_merged);
    MPI_Comm_size(comm_all, &size_merged);
    LOG(3, "comm_all  rank_merged: {}  size_merged: {}", rank_merged,
        size_merged);
  }

  logpid(fmt::format("tmp-pid-worker-{}.txt", rank_merged).c_str());
  LOG(3, "logpid sleep ...");
  sleep_ms(3000);

  auto config_file = jconf["config_file"].GetObject();
  auto shm_fname = config_file["shm"]["fname"].GetString();
  auto shm_size = config_file["shm"]["size"].GetInt64();
  LOG(3, "mmap {} / {}", shm_fname, shm_size);
  auto shm = MMap::create(shm_fname, shm_size);
  LOG(3, "memory ready");

  using namespace FileWriter;

  auto queue = (MsgQueue *)jconf["queue_addr"].GetUint64();
  auto cq = (CollectiveQueue *)jconf["cq_addr"].GetUint64();
  LOG(3, "got cq at: {}", (void *)cq);
  HDFIDStore hdf_store;
  hdf_store.mpi_rank = rank_merged;
  hdf_store.cqid = cq->open();
  LOG(3, "rank_merged: {}  cqid: {}", rank_merged, hdf_store.cqid);

  auto hdf_fname = jconf["hdf"]["fname"].GetString();
  auto hdf_file = std::unique_ptr<HDFFile>(new HDFFile);
  // no need to set the cq ptr on hdffile here, it is just the resource owner in
  // main process.
  hdf_file->reopen(hdf_fname, Value());
  hdf_store.h5file = hdf_file->h5file;

  auto module = jconf["stream"]["module"].GetString();

  auto module_factory = HDFWriterModuleRegistry::find(module);
  if (!module_factory) {
    LOG(5, "Module '{}' is not available", module);
    exit(1);
  }

  auto hdf_writer_module = module_factory();
  if (!hdf_writer_module) {
    LOG(5, "Can not create a HDFWriterModule for '{}'", module);
    exit(1);
  }

  hdf_writer_module->parse_config(jconf["stream"], nullptr);
  hdf_writer_module->reopen(hdf_file->h5file,
                            jconf["stream"]["hdf_parent_name"].GetString(), cq,
                            &hdf_store);
  // jconf["stream"]["hdf_parent_name"].GetString()

  hdf_writer_module->enable_cq(cq, &hdf_store, rank_merged);

  LOG(3, "Barrier 1 BEFORE");
  MPI_Barrier(comm_all);
  LOG(3, "Barrier 1 AFTER");

  auto t_last = CLK::now();

  size_t empties = 0;
  // NOTE
  // This loop will prevent it from running a long time on idle;
  for (int i1 = 0; i1 < 10000; ++i1) {
    auto n = queue->n.load();
    if (n > 0) {
      // reset idle counter
      i1 = 0;
      LOG(9, "Queue size: {}", n);
      std::vector<Msg> all;
      queue->all(all);
      for (auto &m : all) {
        auto t_now = CLK::now();
        // execute all pending commands before the next message
        if (true || t_now - t_last > MS(100)) {
          t_last = t_now;
          LOG(7, "execute collective");
          cq->execute_for(hdf_store);
        }
        // LOG(3, "writing msg  type: {:2}  size: {:5}  data: {}", m.type,
        // m._size, (void*)m.data());
        LOG(9, "hdf_writer_module->write(m) "
               "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
               "~");
        hdf_writer_module->write(m);
      }
    } else {
      if (queue->open != 1) {
        LOG(7, "queue closed");
        break;
      } else {
        if (empties % 1000 == 0) {
          LOG(7, "empty {}", empties);
        }
        empties += 1;
        sleep_ms(1);
      }
    }
  }

  LOG(6, ".....................  wait for  CLOSING");
  cq->barriers[0]++;
  cq->wait_for_barrier(&hdf_store, 0);
  LOG(6, "===============================  CLOSING   "
         "=========================================");

  hdf_writer_module.reset();
  cq->close_for(hdf_store);

  LOG(6, ".....................  wait for  CQ EXEC");
  cq->barriers[1]++;
  cq->wait_for_barrier(&hdf_store, 1);
  LOG(6, "===============================  CQ EXEC   "
         "=========================================");

  cq->execute_for(hdf_store);
  hdf_store.check_all_empty();

  hdf_file.reset();

  LOG(6, "Barrier 2 BEFORE");
  MPI_Barrier(comm_all);
  LOG(6, "Barrier 2 AFTER");

  cq->execute_for(hdf_store);

  LOG(6, "ask for disconnect");
  // MPI_Comm_disconnect(&comm_parent);
  MPI_Comm_disconnect(&comm_all);
  if (false) {
    LOG(6, "finalizing {}", rank_merged);
    MPI_Finalize();
    LOG(6, "after finalize {}", rank_merged);
  }
  LOG(6, "return");
  return 42;

  LOG(3, "wait for parent mmap");
  MPI_Barrier(comm_all);

  MPI_Barrier(comm_all);

  auto m1 = (std::atomic<uint32_t> *)shm->addr();
  while (m1->load() < 110) {
    while (m1->load() % 2 == 0) {
    }
    LOG(3, "store");
    m1->store(m1->load() + 1);
  }
  LOG(3, "final: value: {}", m1->load());

  MPI_Barrier(comm_all);
  LOG(3, "alloc init");

  MPI_Barrier(comm_all);
  LOG(3, "ask for disconnect");
  MPI_Comm_disconnect(&comm_parent);
  MPI_Comm_disconnect(&comm_all);
  LOG(3, "finalizing {}", rank_merged);
  MPI_Finalize();
  LOG(3, "after finalize {}", rank_merged);
  return 42;
}
