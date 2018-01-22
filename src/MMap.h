#pragma once

#include "logger.h"
#include <fcntl.h>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

class MMap {
public:
  using ptr = std::unique_ptr<MMap>;
  using sptr = std::shared_ptr<MMap>;
  using string = std::string;

  static sptr open(string fname, size_t size) {
    return create_inner(fname, size);
  }

  static sptr create(string fname, size_t size) {
    return create_inner(fname, size, true);
  }

  ~MMap() {
    return;
    if (munmap(shm_ptr, shm_size) != 0) {
      LOG(Sev::Error, "munmap failed");
      exit(1);
    }
    shm_ptr = nullptr;
    if (::close(fd) != 0) {
      LOG(Sev::Error, "could not close mmap file");
      exit(1);
    }
    fd = -1;
  }

  void *addr() const { return shm_ptr; }

  size_t size() const { return shm_size; }

private:
  int fd;
  void *shm_ptr;
  size_t shm_size = 0;
  MMap() {}
  static sptr create_inner(string fname, size_t size, bool create = false) {
    static_assert(sizeof(char *) == 8, "requires currently 64 bit pointers");
    auto ret = sptr(new MMap);
    ret->fd = -1;
    ret->shm_ptr = nullptr;
    ret->shm_size = size;
    int flags = O_RDWR;
    if (create) {
      flags |= O_CREAT;
    }
    ret->fd = ::open(fname.data(), flags, S_IRUSR | S_IWUSR);
    if (ret->fd == -1) {
      LOG(Sev::Error, "open failed");
      exit(1);
    }
    if (create) {
      if (ftruncate(ret->fd, ret->shm_size) != 0) {
        LOG(Sev::Error, "fail truncate");
        exit(1);
      }
    }
    ret->shm_ptr =
        mmap64((void *)0x662233000000, ret->shm_size, PROT_READ | PROT_WRITE,
               MAP_FIXED | MAP_SHARED, ret->fd, 0);
    if (ret->shm_ptr == MAP_FAILED) {
      LOG(Sev::Error, "mmap failed");
      exit(1);
    }
    LOG(Sev::Chatty, "shm_ptr: {}", ret->shm_ptr);
    return ret;
  }
};
