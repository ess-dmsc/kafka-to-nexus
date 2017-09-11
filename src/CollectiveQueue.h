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
  uint8_t type = 0;
  union {
    hsize_t set_extent;
  } v;
  std::string to_string() {
    std::string ret;
    switch (type) {
    case 0:
      ret += "Unknown";
      break;
    case 1:
      ret = fmt::format("SetExtent({})", v.set_extent);
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
  std::atomic<uint32_t> open{1};

private:
  pthread_mutex_t mx;
  std::array<CollectiveCommand, 16000> items;
  std::array<size_t, 256> markers;
};
