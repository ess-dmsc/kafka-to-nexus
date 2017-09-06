#pragma once

#include "Msg.h"
#include <mutex>
#include <vector>

class MsgQueue;
void swap(MsgQueue &x, MsgQueue &y);

class MsgQueue {
public:
  using Msg = FileWriter::Msg;
  using LK = std::unique_lock<std::mutex>;
  MsgQueue() {}
  MsgQueue(MsgQueue &&x) { swap(*this, x); }
  void push(Msg &&msg) {
    LK lk(mx);
    items.push_back(std::move(msg));
  }
  std::vector<Msg> all() {
    LK lk(mx);
    std::vector<Msg> ret;
    for (auto &x : items) {
      ret.push_back(std::move(x));
    }
    items.clear();
    return ret;
  }

private:
  std::mutex mx;
  std::vector<Msg> items;
  friend void swap(MsgQueue &x, MsgQueue &y);
};
