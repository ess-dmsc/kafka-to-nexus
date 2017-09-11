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
    if (n >= items.size()) {
      return 1;
    }
    if (pthread_mutex_lock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_lock");
      exit(1);
    }
    // LOG(3, "queuen msg {} / {}", msg.type, msg._size);
    // TODO fix mistake in declaration....
    items[n].swap(msg);
    // LOG(3, "done");
    n += 1;
    // LOG(3, "now have {} in queue", n.load());
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
    return 0;
  }
  void all(std::vector<Msg> &ret) {
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
    for (size_t i1 = 0; i1 < n.load(); ++i1) {
      ret.push_back(std::move(items[i1]));
    }
    /*
    LOG(3, "checking all messages in ret {}", ret.size());
    for (size_t i1 = 0; i1 < ret.size(); ++i1) {
      auto & m = ret[i1];
      LOG(3, "...");
      LOG(3, "msg  type: {:2}  size: {:5}  data: {}", m.type, m._size,
    (void*)m.data());
    }
    */
    n.store(0);
    if (pthread_mutex_unlock(&mx) != 0) {
      LOG(1, "fail pthread_mutex_unlock");
      exit(1);
    }
  }
  std::atomic<size_t> n{0};
  std::atomic<uint32_t> open{1};

private:
  pthread_mutex_t mx;
  std::array<Msg, 256> items;
  friend void swap(MsgQueue &x, MsgQueue &y);
};
