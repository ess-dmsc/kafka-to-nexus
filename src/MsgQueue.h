#pragma once

#include "Msg.h"
#include <atomic>
#include <memory>
#include <pthread.h>
#include <vector>

class MsgQueue;
void swap(MsgQueue &x, MsgQueue &y);

class MsgQueue {
public:
  using ptr = std::unique_ptr<MsgQueue>;
  using Msg = FileWriter::Msg;
  MsgQueue() {
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
  ~MsgQueue() {
    if (pthread_mutex_destroy(&mx) != 0) {
      LOG(3, "fail pthread_mutex_destroy");
      exit(1);
    }
  }
  MsgQueue(MsgQueue &x) { swap(*this, x); }
  int push(Msg &msg) {
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    auto nW = nw.load();
    auto nR = nr.load();
    auto nn = size(nW, nR);
    if (nn >= items.size() - 1) {
      if (pthread_mutex_unlock(&mx) != 0) {
        LOG(1, "fail pthread_mutex_unlock");
        exit(1);
      }
      return nn;
    }
    // LOG(3, "queuen msg {} / {}", msg.type, msg._size);
    items[nW].swap(msg);
    nW = (nW + 1) % items.size();
    nw.store(nW);
    // LOG(3, "now have {} in queue", n.load());
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
    return 0;
  }
  void all(std::vector<Msg> &ret, size_t fac) {
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    /*
    LOG(3, "checking all messages before move");
    for (size_t i1 = 0; i1 < n.load(); ++i1) {
      auto & m = items[i1];
      LOG(3, "msg  type: {:2}  size: {:5}  data: {}", m.type, m._size,
    (void*)m.data());
    }
    */
    auto nW = nw.load();
    auto nR = nr.load();
    auto nn = size(nW, nR);
    size_t c1 = nn;
    if (c1 > items.size() / fac) {
      c1 = items.size() / fac;
    }
    for (size_t i1 = 0; i1 < c1; ++i1) {
      ret.push_back(std::move(items[nR]));
      nR = (nR + 1) % items.size();
    }
    nr.store(nR);
    /*
    LOG(3, "checking all messages in ret {}", ret.size());
    for (size_t i1 = 0; i1 < ret.size(); ++i1) {
      auto & m = ret[i1];
      LOG(3, "...");
      LOG(3, "msg  type: {:2}  size: {:5}  data: {}", m.type, m._size,
    (void*)m.data());
    }
    */
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
  }
  size_t size(size_t nW, size_t nR) {
    size_t nn = 0;
    if (nW > nR) {
      nn = nW - nR;
    } else if (nW < nR) {
      nn = items.size() - nR + nW;
    }
    return nn;
  }
  std::atomic<size_t> nw{0};
  std::atomic<size_t> nr{0};
  std::atomic<uint32_t> open{1};

private:
  pthread_mutex_t mx;
  std::array<Msg, (2 << 10)> items;
  friend void swap(MsgQueue &x, MsgQueue &y);
};
