#pragma once

#include <jemalloc/jemalloc.h>
#include <map>
#include <memory>
#include <mutex>

static void jemcb(void *cbd, char const *s) { fwrite(s, 1, strlen(s), stdout); }

class Jemalloc;

static std::map<void *, Jemalloc *> g_jems;
static std::mutex g_mx;

static void *extent_alloc(extent_hooks_t *extent_hooks, void *addr, size_t size,
                          size_t align, bool *zero, bool *commit,
                          unsigned arena);

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
  using sptr = std::shared_ptr<Jemalloc>;

  static sptr create(void *base, void *ceil) {
    auto ret = sptr(new Jemalloc);
    ret->base = base;
    ret->alloc_base = ret->base;
    ret->alloc_now = ret->base;
    ret->alloc_ceil = ceil;
    int err = 0;
    char const *jemalloc_version = nullptr;
    ;
    size_t n = 0;
    n = sizeof(char const *);
    mallctl("version", &jemalloc_version, &n, nullptr, 0);
    LOG(3, "jemalloc version: {}", jemalloc_version);

    n = sizeof(unsigned);
    mallctl("thread.arena", &ret->default_thread_arena, &n, nullptr, 0);
    LOG(3, "thread.arena: {}", ret->default_thread_arena);

    if (false) {
      err = mallctl("arena.0.destroy", nullptr, nullptr, nullptr, 0);
      if (err != 0) {
        LOG(3, "could not destroy arena");
        exit(1);
      }
    }

    unsigned narenas = 0;
    n = sizeof(narenas);
    mallctl("arenas.narenas", &narenas, &n, nullptr, 0);
    LOG(3, "arenas.narenas: {}", narenas);

    // tcache_flush();

    std::memset(&ret->hooks, 0, sizeof(extent_hooks_t));
    auto self = ret.get();
    {
      std::unique_lock<std::mutex> lock(g_mx);
      g_jems[&ret->hooks] = self;
    }
    ret->hooks.alloc = extent_alloc;
    extent_hooks_t *hooks_ptr = &ret->hooks;
    n = sizeof(unsigned);
    err = mallctl("arenas.create", &ret->aix, &n, &hooks_ptr,
                  sizeof(extent_hooks_t *));
    // int err = mallctl("arenas.create", &aix, &n, nullptr, 0);
    if (err != 0) {
      LOG(3, "error in mallctl arenas.create: {}", errname(err));
      exit(1);
    }
    LOG(3, "arena created: {}", ret->aix);

    // tcache_flush();

    // void * big = mallocx(80 * 1024 * 1024, );

    return ret;
  }

  void stats() const { malloc_stats_print(jemcb, nullptr, ""); }

  static void tcache_flush() {
    int err = 0;
    err = mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    if (err != 0) {
      LOG(3, "fail thread.tcache.flush");
      exit(1);
    }
  }

  static void tcache_disable() {
    int err = 0;
    bool v = false;
    size_t n = sizeof(bool);
    err = mallctl("thread.tcache.enabled", &v, &n, nullptr, 0);
    if (err != 0) {
      LOG(3, "fail thread.tcache.enabled");
      exit(1);
    }
  }

  void *alloc(size_t size) {
    // LOG(3, "alloc from arena {}", aix);
    auto addr = mallocx(size, MALLOCX_ARENA(aix));
    if (addr == nullptr) {
      LOG(3, "fail alloc size: {}", size);
      exit(1);
    }
    return addr;
  }

  void use_this() {
    size_t n = sizeof(unsigned);
    int err;
    err = mallctl("thread.arena", nullptr, nullptr, &aix, n);
    if (err != 0) {
      LOG(3, "can not set arena");
      exit(1);
    }
    LOG(3, "use_this: {}", aix);
    Jemalloc::tcache_flush();
  }

  void use_default() {
    size_t n = sizeof(unsigned);
    int err;
    err = mallctl("thread.arena", nullptr, nullptr, &default_thread_arena, n);
    if (err != 0) {
      LOG(3, "can not set arena");
      exit(1);
    }
    LOG(3, "use_default: {}", default_thread_arena);
    Jemalloc::tcache_flush();
  }

  bool check_in_range(void *p) {
    using P = uint8_t *;
    bool ret = P(p) >= P(alloc_base) && P(p) < P(alloc_ceil);
    // LOG(3, "check_in_range: {}  {}  {}  {}", ret, alloc_base, p, alloc_ceil);
    return ret;
  }

  void *alloc_base = nullptr;
  void *alloc_now = nullptr;
  void *alloc_ceil = nullptr;

private:
  Jemalloc() {}
  unsigned aix = -1;
  void *base = nullptr;
  extent_hooks_t hooks;
  std::function<void *(extent_hooks_t *extent_hooks, void *addr, size_t size,
                       size_t align, bool *zero, bool *commit, unsigned arena)>
      f_alloc;
  unsigned default_thread_arena = -1;
};

static void *extent_alloc(extent_hooks_t *extent_hooks, void *addr, size_t size,
                          size_t align, bool *zero, bool *commit,
                          unsigned arena) {
  // LOG(3, "extent_alloc arena: {}  size: {:8}  align: {:4}  zero: {:5}
  // commit: {:5}", arena, size, align, *zero, *commit);
  if (addr != nullptr) {
    LOG(3, "error addr is set");
    exit(1);
  }
  std::unique_lock<std::mutex> lock(g_mx);
  auto jm = g_jems[extent_hooks];
  auto now = (char const *)jm->alloc_now;
  auto ceil = (char const *)jm->alloc_ceil;
  if (now + size > ceil) {
    LOG(3, "sorry, no more extents");
    exit(1);
    return nullptr;
  }
  if (size_t(now) % align != 0) {
    // let's see if that ever happens in tests...
    LOG(3, "sorry, bad alignment");
    exit(1);
  }
  auto q = (void *)now;
  // LOG(3, "extent: {}", q);
  now += size;
  jm->alloc_now = (void *)now;
  if (*zero) {
    std::memset(q, 0, size);
  }
  return q;
}
