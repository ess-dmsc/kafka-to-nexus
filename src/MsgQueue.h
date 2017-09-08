#pragma once

#include "Msg.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

class MsgQueue;
void swap(MsgQueue &x, MsgQueue &y);

class MsgQueue {
public:
  using ptr = std::unique_ptr<MsgQueue>;
  using Msg = FileWriter::Msg;
  using LK = std::unique_lock<std::mutex>;
  MsgQueue() {}
  MsgQueue(MsgQueue &x) { swap(*this, x); }
  int push(Msg &msg) {
    LK lk(mx);
    if (n >= items.size()) {
      return 1;
    }
    // LOG(3, "queuen msg {} / {}", msg.type, msg._size);
    // TODO fix mistake in declaration....
    items[n].swap(msg);
    // LOG(3, "done");
    n += 1;
    // LOG(3, "now have {} in queue", n.load());
    return 0;
  }
  void all(std::vector<Msg> &ret) {
    LK lk(mx);
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
  }
  std::atomic<size_t> n{0};
  std::atomic<uint32_t> open{1};

private:
  std::mutex mx;
  std::array<Msg, 256> items;
  friend void swap(MsgQueue &x, MsgQueue &y);
};
