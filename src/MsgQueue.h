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
  MsgQueue(MsgQueue &&x) { swap(*this, x); }
  int push(Msg &&msg) {
    LK lk(mx);
    if (n >= items.size()) {
      return 1;
    }
    // TODO fix mistake in declaration....
    items[n].swap(items[n], msg);
    n += 1;
    LOG(3, "now have {} in queue", n.load());
    return 0;
  }
  std::vector<Msg> all() {
    LK lk(mx);
    std::vector<Msg> ret;
    for (auto &x : items) {
      ret.push_back(std::move(x));
    }
    return ret;
  }

private:
  std::mutex mx;
  std::array<Msg, 1024> items;
  std::atomic<size_t> n{0};
  friend void swap(MsgQueue &x, MsgQueue &y);
};
