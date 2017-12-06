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

int main(int argc, char **argv) {
  int err = MPI_SUCCESS;
  err = MPI_Init(&argc, &argv);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Init");
    exit(1);
  }

  int rank_world, size_world;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank_world);
  MPI_Comm_size(MPI_COMM_WORLD, &size_world);
  LOG(3, "mpi-worker  rank_world: {}  size_world: {}", rank_world, size_world);
  MPI_Comm comm_parent;
  err = MPI_Comm_get_parent(&comm_parent);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_get_parent");
    exit(1);
  }

  {
    int rank, size;
    MPI_Comm_rank(comm_parent, &rank);
    MPI_Comm_size(comm_parent, &size);
    LOG(3, "comm_parent rank: {}  size: {}", rank, size);
  }

  using rapidjson::Value;
  rapidjson::Document jconf;
  {
    MPI_Status status;
    std::vector<char> buf;
    buf.resize(1024 * 1024);
    err = MPI_Recv(buf.data(), buf.size(), MPI_CHAR, MPI_ANY_SOURCE, 101,
                   comm_parent, &status);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Recv");
      exit(1);
    }
    int size = -1;
    err = MPI_Get_count(&status, MPI_CHAR, &size);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Get_count");
      exit(1);
    }
    jconf.Parse(buf.data(), buf.size());
    if (jconf.HasParseError()) {
      LOG(3, "can not parse the command");
      exit(1);
    }
  }

  if (auto x = get_int(&jconf, "log_level")) {
    log_level = x.v;
  }

  int rank_merged, size_merged;
  MPI_Comm comm_all;
  int comm_all_rank = -1;
  {
    err = MPI_Intercomm_merge(comm_parent, 1, &comm_all);
    if (err != MPI_SUCCESS) {
      LOG(3, "fail MPI_Intercomm_merge");
      exit(1);
    }
    MPI_Comm_rank(comm_all, &rank_merged);
    MPI_Comm_size(comm_all, &size_merged);
    LOG(8, "comm_all  rank_merged: {}  size_merged: {}", rank_merged,
        size_merged);
    comm_all_rank = rank_merged;
  }

  if (jconf.FindMember("logpid-sleep") != jconf.MemberEnd()) {
    logpid(fmt::format("tmp-pid-worker-{}.txt", rank_merged).c_str());
    LOG(7, "logpid sleep ...");
    sleep_ms(3000);
  }

  auto config_file = jconf["config_file"].GetObject();
  auto shm_fname = config_file["shm"]["fname"].GetString();
  auto shm_size = config_file["shm"]["size"].GetInt64();
  auto shm = MMap::create(shm_fname, shm_size);

  using namespace FileWriter;

  auto queue = (MsgQueue *)jconf["queue_addr"].GetUint64();
  auto cq = (CollectiveQueue *)jconf["cq_addr"].GetUint64();
  HDFIDStore hdf_store;
  hdf_store.mpi_rank = rank_merged;
  hdf_store.cqid = cq->open(hdf_store);
  LOG(8, "rank_merged: {}  cqid: {}", rank_merged, hdf_store.cqid);

  auto hdf_fname = jconf["hdf"]["fname"].GetString();
  auto hdf_file = std::unique_ptr<HDFFile>(new HDFFile);
  hdf_file->cq = cq;
  LOG(8, "hdf_file->reopen()  {}", hdf_fname);
  hdf_file->reopen(hdf_fname, Value());
  hdf_store.h5file = hdf_file->h5file;

  auto module = jconf["stream"]["module"].GetString();

  LOG(8, "HDFWriterModuleRegistry::find(module)  {}", module);
  auto module_factory = HDFWriterModuleRegistry::find(module);
  if (!module_factory) {
    LOG(5, "Module '{}' is not available", module);
    exit(1);
  }

  LOG(8, "module_factory()");
  auto hdf_writer_module = module_factory();
  if (!hdf_writer_module) {
    LOG(5, "Can not create a HDFWriterModule for '{}'", module);
    exit(1);
  }

  LOG(8, "hdf_writer_module->parse_config()");
  hdf_writer_module->parse_config(jconf["stream"], nullptr);
  LOG(8, "hdf_writer_module->reopen()");
  hdf_writer_module->reopen(hdf_file->h5file,
                            jconf["stream"]["hdf_parent_name"].GetString(), cq,
                            &hdf_store);
  // jconf["stream"]["hdf_parent_name"].GetString()

  LOG(8, "hdf_writer_module->enable_cq()");
  hdf_writer_module->enable_cq(cq, &hdf_store, rank_merged);

  LOG(8, "Barrier 1 BEFORE");
  MPI_Barrier(comm_all);
  LOG(8, "Barrier 1 AFTER");

  auto t_last = CLK::now();

  size_t empties = 0;
  // NOTE
  // This loop will prevent it from running a long time on idle;
  for (int i1 = 0; i1 < 10000; ++i1) {
    std::vector<Msg> all;
    queue->all(all, size_merged);
    if (all.size() > 0) {
      // reset idle counter
      i1 = 0;
      for (auto &m : all) {
        auto t_now = CLK::now();
        // execute all pending commands before the next message
        if (true || t_now - t_last > MS(100)) {
          t_last = t_now;
          cq->execute_for(hdf_store, 0);
        }
        // LOG(9, "writing msg  type: {:2}  size: {:5}  data: {}", m.type,
        // m._size, (void*)m.data());
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

  auto barrier = [&cq, &hdf_store](size_t id, size_t queue, std::string name) {
    LOG(8, "...............................  cqid: {}  wait   {}  {}",
        hdf_store.cqid, id, name);
    cq->barriers[id]++;
    cq->wait_for_barrier(&hdf_store, id, queue);
    LOG(8, "===============================  cqid: {}  after  {}  {}",
        hdf_store.cqid, id, name);
  };

  barrier(0, 0, "MODULE RESET");
  hdf_writer_module.reset();
  cq->close_for(hdf_store);

  barrier(1, 0, "CQ EXEC");

  barrier(2, 1, "CQ EXEC 2");

  barrier(5, 2, "CQ EXEC 3");

  LOG(8, "check_all_empty");
  hdf_store.check_all_empty();

  LOG(8, "hdf_file.reset()");
  hdf_file.reset();

  barrier(3, 2, "MPI Barrier");
  err = MPI_Barrier(comm_all);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Barrier");
    exit(1);
  }

  LOG(8, "ask for disconnect  cqid: {}", hdf_store.cqid);
  err = MPI_Comm_disconnect(&comm_all);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
  }
  LOG(8, "ask for disconnect  cqid: {}", hdf_store.cqid);
  err = MPI_Comm_disconnect(&comm_parent);
  if (err != MPI_SUCCESS) {
    LOG(3, "fail MPI_Comm_disconnect");
  }

  barrier(4, -1, "Last CQ barrier");
  LOG(8, "finalizing {}", rank_merged);
  MPI_Finalize();
  LOG(8, "after finalize {}", rank_merged);
}
