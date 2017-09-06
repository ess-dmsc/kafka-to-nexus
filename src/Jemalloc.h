#pragma once

#include <jemalloc/jemalloc.h>
#include <memory>

static void jemcb(void *cbd, char const *s) { fwrite(s, 1, strlen(s), stdout); }

static void *g_alloc_base = nullptr;
static void *g_alloc_ceil = nullptr;

static void *extent_alloc(extent_hooks_t *extent_hooks, void *addr, size_t size,
                          size_t align, bool *zero, bool *commit,
                          unsigned arena) {
  LOG(3, "extent_alloc arena: {}  size: {}  align: {}  zero: {}  commit: {}",
      arena, size, align, *zero, *commit);
  void *q = new char[size];
  if (*zero) {
    std::memset(q, 0, size);
  }
  if (addr != nullptr) {
    LOG(3, "error addr is set");
    exit(1);
  }
  return q;
}

static char const *errname(int err) {
  switch (err) {
  case EINVAL:
    return "EINVAL";
  case ENOENT:
    return "ENOENT";
  case EPERM:
    return "EPERM";
  case EAGAIN:
    return "EAGAIN";
  case EFAULT:
    return "EFAULT";
  }
  return "UNKNOWN";
}

class Jemalloc {
public:
  using ptr = std::unique_ptr<Jemalloc>;

  static ptr create(void *base, void *ceil) {
    auto ret = ptr(new Jemalloc);
    ret->base = base;
    g_alloc_base = ret->base;
    g_alloc_ceil = ceil;
    char const *jemalloc_version = nullptr;
    ;
    size_t n = 0;
    n = sizeof(char const *);
    mallctl("version", &jemalloc_version, &n, nullptr, 0);
    LOG(3, "jemalloc version: {}", jemalloc_version);
    unsigned narenas = 0;
    n = sizeof(narenas);
    mallctl("arenas.narenas", &narenas, &n, nullptr, 0);
    LOG(3, "arenas.narenas: {}", narenas);

    extent_hooks_t hooks;
    std::memset(&hooks, 0, sizeof(extent_hooks_t));
    hooks.alloc = extent_alloc;
    extent_hooks_t *hooks_ptr = &hooks;
    n = sizeof(unsigned);
    int err = mallctl("arenas.create", &ret->aix, &n, &hooks_ptr,
                      sizeof(extent_hooks_t *));
    // int err = mallctl("arenas.create", &aix, &n, nullptr, 0);
    if (err != 0) {
      LOG(3, "error in mallctl arenas.create: {}", errname(err));
      exit(1);
    }
    LOG(3, "arena created: {}", ret->aix);

    // void * big = mallocx(80 * 1024 * 1024, );

    return ret;
  }

  void stats() const { malloc_stats_print(jemcb, nullptr, ""); }

  void *alloc(size_t size) {
    auto addr = mallocx(size, MALLOCX_ARENA(aix));
    if (addr == nullptr) {
      LOG(3, "fail alloc size: {}", size);
      exit(1);
    }
    return addr;
  }

private:
  Jemalloc() {}
  unsigned aix = -1;
  void *base = nullptr;
};
