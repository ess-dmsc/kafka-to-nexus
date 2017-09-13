#pragma once

#include "Jemalloc.h"
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
};

enum struct CollectiveCommandType : uint8_t {
  Unknown,
  SetExtent,
  H5Dopen2,
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

  static CollectiveCommand H5Dopen2(hid_t loc, char const *name) {
    CollectiveCommand ret;
    ret.type = CollectiveCommandType::H5Dopen2;
    ret.v.H5Dopen2.loc = loc;
    strncpy(ret.v.H5Dopen2.name, name, STR_NAME_MAX);
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
      hid_t loc;
      char name[STR_NAME_MAX];
    } H5Dopen2;
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
      ret = fmt::format("H5Dopen2({}, {})", v.H5Dopen2.loc, v.H5Dopen2.name);
      break;
    default:
      LOG(3, "unhandled");
      exit(1);
    }
    return ret;
  }

  void execute_for(HDFIDStore &store) {
    if (type == CollectiveCommandType::SetExtent) {
      LOG(3, "execute {}", to_string());
      LOG(3, "not implemented");
      exit(1);
      // check in store if already open, then use that id.
      // if not, open and keep in cache. (but do not issue a close later! h5d
      // classes will issue the close)
      /*
      herr_t err = 0;
      hid_t id = -1;
      hid_t dsp_tgt = -1;
      std::array<hsize_t, 2> sext;
      std::array<hsize_t, 2> smax;
      id = H5Dopen2(h5file, v.set_extent.name, H5P_DEFAULT);
      if (id < 0) {
        LOG(3, "H5Dopen2 failed");
        exit(1);
      }
      dsp_tgt = H5Dget_space(id);
      H5Sget_simple_extent_dims(dsp_tgt, sext.data(), smax.data());
      sext[0] = v.set_extent.size[0];
      err = H5Dset_extent(id, sext.data());
      if (err < 0) {
        LOG(3, "fail H5Dset_extent");
        exit(1);
      }
      H5Dclose(id);
      if (err < 0) {
        LOG(3, "fail H5Dclose");
        exit(1);
      }
      */
    } else if (type == CollectiveCommandType::H5Dopen2) {
      auto id = ::H5Dopen2(v.H5Dopen2.loc, v.H5Dopen2.name, H5P_DEFAULT);
      if (id < 0) {
        LOG(3, "H5Dopen2 failed");
      }
      char buf[512];
      {
        auto bufn = H5Iget_name(id, buf, 512);
        buf[bufn] = '\0';
      }
      LOG(3, "opened as name: {}", buf);
      store.datasetname_to_ds_id[buf] = id;
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
    auto n1 = n.load();
    if (n1 >= items.size()) {
      LOG(3, "Command queue full");
      exit(1);
      return 1;
    }
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    LOG(3, "push new ccmd: {}", item.to_string());
    items[n1] = item;
    n1 += 1;
    // LOG(3, "now have {} in queue", n.load());

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
    LOG(3, "execute_for  cqid: {}", store.cqid);
    std::vector<CollectiveCommand> cmds;
    all_for(store, cmds);
    LOG(3, "execute_for  cqid: {}  cmds: {}", store.cqid, cmds.size());
    for (auto &cmd : cmds) {
      cmd.execute_for(store);
    }
  }

  void register_datasetname(std::string name) {
    auto n = datasetname_to_snow_a_ix__n++;
    LOG(3, "register dataset {} as snow_a_ix {}", name, n);
    std::strncpy(datasetname_to_snow_a_ix_name[n].data(), name.data(), 256);
  }

  size_t find_snowix_for_datasetname(std::string name) {
    LOG(3, "find_snowix_for_datasetname {}", name);
    for (size_t i1 = 0; i1 < datasetname_to_snow_a_ix__n; ++i1) {
      if (std::strncmp(name.data(), datasetname_to_snow_a_ix_name[i1].data(),
                       256) == 0) {
        LOG(3, "found ix: {}", i1);
        return i1;
      }
    }
    LOG(3, "error not found");
    exit(1);
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
};
