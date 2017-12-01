#pragma once

#include "logger.h"
#include <map>
#include <memory>
#include <mutex>

class Jemalloc {
public:
  using ptr = std::unique_ptr<Jemalloc>;
  using sptr = std::shared_ptr<Jemalloc>;

  static sptr create(void *base, void *ceil) {
    auto ret = sptr(new Jemalloc);
    return ret;
  }

  void stats() const {}

  static void tcache_flush() {}

  static void tcache_disable() {}

  void *alloc(size_t size) {
    auto addr = malloc(size);
    if (addr == nullptr) {
      LOG(3, "fail alloc size: {}", size);
      exit(1);
    }
    return addr;
  }

  void use_this() {}

  void use_default() {}

  bool check_in_range(void *p) { return true; }

private:
  Jemalloc() {}
};
