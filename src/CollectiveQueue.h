#pragma once

#include "logger.h"
#include <atomic>
#include <hdf5.h>
#include <memory>
#include <pthread.h>
#include <string>
#include <vector>

enum struct CollectiveCommandType : uint8_t {
  Unknown,
  SetExtent,
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

  CollectiveCommandType type = CollectiveCommandType::Unknown;

  union {
    struct {
      char name[STR_NAME_MAX];
      hsize_t ndims;
      hsize_t size[8];
    } set_extent;
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
    default:
      LOG(3, "unhandled");
      exit(1);
    }
    return ret;
  }
};

class CollectiveQueue {
public:
  using ptr = std::unique_ptr<CollectiveQueue>;

  CollectiveQueue() {
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

  void all_for(size_t ix, std::vector<CollectiveCommand> &ret) {
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    auto n1 = n.load();
    for (size_t i1 = markers.at(ix); i1 < n1; ++i1) {
      ret.push_back(items[i1]);
    }
    markers.at(ix) = n1;
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
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

public:
  // hitch-hiker:
  using AT = std::atomic<size_t>;
  std::array<AT, 1024> snow;
};
