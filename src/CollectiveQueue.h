#pragma once

#include "Jemalloc.h"
#include "helper.h"
#include "logger.h"
#include <atomic>
#include <hdf5.h>
#include <memory>
#include <pthread.h>
#include <string>
#include <vector>

// a per-worker store

struct HDFIDStore {
  std::map<std::string, hid_t> datasetname_to_ds_id;
  std::map<std::string, hid_t> datasetname_to_dsp_id;
  size_t cqid = -1;
  hid_t h5file = -1;
  int mpi_rank = -1;

  void check_all_empty() {
    if (datasetname_to_ds_id.size() != 0) {
      LOG(3, "datasetname_to_ds_id is not empty");
      exit(1);
    }
    if (datasetname_to_dsp_id.size() != 0) {
      LOG(3, "datasetname_to_dsp_id is not empty");
      exit(1);
    }
  }
};

enum struct CollectiveCommandType : uint8_t {
  Unknown,
  SetExtent,
  H5Dopen2,
  H5Dclose,
};

struct CollectiveCommand {

  static size_t const STR_NAME_MAX = 200;

  static CollectiveCommand set_extent(char const *name, hsize_t const ndims,
                                      hsize_t const *size) {
    CollectiveCommand ret;
    ret.type = CollectiveCommandType::SetExtent;
    strncpy(ret.v.set_extent.name, name, STR_NAME_MAX);
    ret.v.set_extent.ndims = ndims;
    for (size_t i1 = 0; i1 < ndims; ++i1) {
      ret.v.set_extent.size[i1] = size[i1];
    }
    return ret;
  }

  static CollectiveCommand H5Dopen2(char const *name) {
    CollectiveCommand ret;
    ret.type = CollectiveCommandType::H5Dopen2;
    strncpy(ret.v.H5Dopen2.name, name, STR_NAME_MAX);
    return ret;
  }

  static CollectiveCommand H5Dclose(char const *name) {
    CollectiveCommand ret;
    ret.type = CollectiveCommandType::H5Dclose;
    strncpy(ret.v.H5Dclose.name, name, STR_NAME_MAX);
    return ret;
  }

  CollectiveCommandType type = CollectiveCommandType::Unknown;

  union {
    struct {
      char name[STR_NAME_MAX];
      hsize_t ndims;
      hsize_t size[8];
    } set_extent;
    struct {
      char name[STR_NAME_MAX];
    } H5Dopen2;
    struct {
      char name[STR_NAME_MAX];
    } H5Dclose;
  } v;

  bool done = false;

  std::string to_string() {
    std::string ret;
    switch (type) {
    case CollectiveCommandType::Unknown:
      ret += "Unknown";
      break;
    case CollectiveCommandType::SetExtent:
      ret = fmt::format("SetExtent({}, {}, {})", v.set_extent.name,
                        v.set_extent.ndims, v.set_extent.size[0]);
      break;
    case CollectiveCommandType::H5Dopen2:
      ret = fmt::format("H5Dopen2({})", v.H5Dopen2.name);
      break;
    case CollectiveCommandType::H5Dclose:
      ret = fmt::format("H5Dclose({})", v.H5Dclose.name);
      break;
    default:
      LOG(3, "unhandled");
      exit(1);
    }
    return ret;
  }

  bool equivalent(CollectiveCommand &x) {
    if (type != x.type)
      return false;
    switch (type) {
    case CollectiveCommandType::SetExtent:
      if (strncmp(v.set_extent.name, x.v.set_extent.name, STR_NAME_MAX) != 0) {
        return false;
      }
      if (v.set_extent.ndims != x.v.set_extent.ndims) {
        return false;
      }
      if (v.set_extent.size[0] != x.v.set_extent.size[0]) {
        return false;
      }
      break;
    case CollectiveCommandType::H5Dopen2:
      if (strncmp(v.H5Dopen2.name, x.v.H5Dopen2.name, STR_NAME_MAX) != 0) {
        return false;
      }
      break;
    case CollectiveCommandType::H5Dclose:
      if (strncmp(v.H5Dclose.name, x.v.H5Dclose.name, STR_NAME_MAX) != 0) {
        return false;
      }
      break;
    default:
      LOG(3, "unhandled");
      exit(1);
    }
    return true;
  }

  void execute_for(HDFIDStore &store) {
    if (type == CollectiveCommandType::SetExtent) {
      LOG(8, "execute {}", to_string());
      // the dataset must be open because it must have been created by another
      // collective call.
      herr_t err = 0;
      hid_t id = store.datasetname_to_ds_id[v.set_extent.name];
      // hid_t dsp_tgt = store.datasetname_to_dsp_id[v.set_extent.name];
      // std::array<hsize_t, 2> sext;
      // std::array<hsize_t, 2> smax;
      // This seems to trigger a MPI send, recv and barrier!
      err = H5Dset_extent(id, v.set_extent.size);
      if (err < 0) {
        LOG(3, "fail H5Dset_extent");
        exit(1);
      }
      store.datasetname_to_dsp_id[v.set_extent.name] = H5Dget_space(id);
    } else if (type == CollectiveCommandType::H5Dopen2) {
      auto id = ::H5Dopen2(store.h5file, v.H5Dopen2.name, H5P_DEFAULT);
      if (id < 0) {
        LOG(3, "H5Dopen2 failed");
      }
      char buf[512];
      {
        auto bufn = H5Iget_name(id, buf, 512);
        buf[bufn] = '\0';
      }
      LOG(8, "opened for {:2} as name: {}  id: {}", store.mpi_rank, buf, id);
      store.datasetname_to_ds_id[buf] = id;
      store.datasetname_to_dsp_id[buf] = H5Dget_space(id);
    } else if (type == CollectiveCommandType::H5Dclose) {
      auto &name = v.H5Dclose.name;
      auto id = ::H5Dclose(store.datasetname_to_ds_id[name]);
      if (id < 0) {
        LOG(3, "H5Dclose failed");
      }
      store.datasetname_to_ds_id.erase(name);
      store.datasetname_to_dsp_id.erase(name);
    } else {
      LOG(3, "unhandled");
      exit(1);
    }
  }
};

class CollectiveQueue {
public:
  using ptr = std::unique_ptr<CollectiveQueue>;

  CollectiveQueue(Jemalloc::sptr jm) : jm(jm) {
    std::memset(markers.data(), 0, markers.size());
    for (auto &x : mark_open) {
      x = false;
    }
    for (auto &x : snow) {
      x.store(0);
    }
    for (auto &x : barriers) {
      x.store(0);
    }
    pthread_mutexattr_t mx_attr;
    if (pthread_mutexattr_init(&mx_attr) != 0) {
      LOG(3, "fail pthread_mutexattr_init");
      exit(1);
    }
    if (pthread_mutexattr_setpshared(&mx_attr, PTHREAD_PROCESS_SHARED) != 0) {
      LOG(3, "fail pthread_mutexattr_setpshared");
      exit(1);
    }
    if (pthread_mutex_init(&mx, &mx_attr) != 0) {
      LOG(3, "fail pthread_mutex_init");
      exit(1);
    }
    if (pthread_mutexattr_destroy(&mx_attr) != 0) {
      LOG(3, "fail pthread_mutexattr_destroy");
      exit(1);
    }
  }

  ~CollectiveQueue() {
    if (pthread_mutex_destroy(&mx) != 0) {
      LOG(3, "fail pthread_mutex_destroy");
      exit(1);
    }
  }

  size_t open() {
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    auto n = nclients;
    mark_open[n] = true;
    markers[n] = 0;
    nclients += 1;
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
    return n;
  }

  int push(CollectiveCommand item) {
    if (n.load() >= items.size()) {
      LOG(3, "Command queue full");
      exit(1);
      return 1;
    }
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    auto n1 = n.load();

    bool do_insert = true;
    // check for doubles
    {
      for (size_t i1 = 0; i1 < n1; ++i1) {
        if (item.equivalent(items[i1])) {
          // LOG(7, "found equivalent command");
          do_insert = false;
          break;
        }
      }
    }

    if (do_insert) {
      LOG(8, "push new ccmd  cmd: {}", item.to_string());
      items[n1] = item;
      n1 += 1;
      LOG(8, "now have {} in queue", n.load());
    }

    // TODO
    // trigger cleanup when reaching queue full
    //   Remove old items, move remaining ones
    //   Move all markers
    //   Set the new 'n'

    n.store(n1);
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
    return 0;
  }

  void all_for(HDFIDStore &store, std::vector<CollectiveCommand> &ret) {
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    auto n1 = n.load();
    for (size_t i1 = markers.at(store.cqid); i1 < n1; ++i1) {
      ret.push_back(items[i1]);
    }
    markers.at(store.cqid) = n1;
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
  }

  void execute_for(HDFIDStore &store) {
    LOG(9, "execute_for  cqid: {}", store.cqid);
    std::vector<CollectiveCommand> cmds;
    all_for(store, cmds);
    LOG(9, "execute_for  cqid: {}  cmds: {}", store.cqid, cmds.size());
    for (auto &cmd : cmds) {
      cmd.execute_for(store);
    }
  }

  void register_datasetname(std::string name) {
    auto n = datasetname_to_snow_a_ix__n++;
    LOG(7, "register dataset {} as snow_a_ix {}", name, n);
    std::strncpy(datasetname_to_snow_a_ix_name[n].data(), name.data(), 256);
  }

  size_t find_snowix_for_datasetname(std::string name) {
    LOG(9, "find_snowix_for_datasetname {}", name);
    for (size_t i1 = 0; i1 < datasetname_to_snow_a_ix__n; ++i1) {
      if (std::strncmp(name.data(), datasetname_to_snow_a_ix_name[i1].data(),
                       256) == 0) {
        LOG(9, "found ix: {}", i1);
        return i1;
      }
    }
    LOG(3, "error not found");
    exit(1);
  }

  void close_for(HDFIDStore &store) { mark_open.at(store.cqid) = false; }

  void wait_for_barrier(HDFIDStore *store, size_t i) {
    auto &n = barriers.at(i);
    int i1 = 0;
    while (true) {
      auto n1 = n.load();
      LOG(9, "spinlock: {} vs {}", n1, nclients);
      if (n1 == nclients) {
        break;
      }
      if (store) {
        execute_for(*store);
      }
      sleep_ms(10);
      if (i1 > 1000) {
        LOG(3, "timeout");
        exit(1);
      }
      i1 += 1;
    }
  }

  std::atomic<size_t> n{0};

private:
  pthread_mutex_t mx;
  static size_t const ITEMS_MAX = 16000;
  std::array<CollectiveCommand, ITEMS_MAX> items;
  // Mark position for each participant
  std::array<size_t, 256> markers;
  std::array<bool, 256> mark_open;
  size_t nclients = 0;
  Jemalloc::sptr jm;

public:
  // hitch-hiker:
  using AT = std::atomic<size_t>;
  std::array<AT, 1024> snow;
  std::array<std::array<char, 256>, 32> datasetname_to_snow_a_ix_name;
  size_t datasetname_to_snow_a_ix__n = 0;

  std::array<std::atomic<int>, 8> barriers;
};
