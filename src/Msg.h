#pragma once

#include "Jemalloc.h"
#include "logger.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

namespace FileWriter {

class Msg {
public:
  Msg() { type = -1; }

  static Msg owned(char const *data, size_t len) {
    Msg msg;
    msg.type = 0;
    msg.var.owned = new char[len];
    std::memcpy((void *)msg.var.owned, data, len);
    msg._size = len;
    return msg;
  }

  static Msg shared(char const *data, size_t len,
                    std::shared_ptr<Jemalloc> &jm) {
    char *p1;
    while (true) {
      p1 = (char *)jm->alloc(len * sizeof(char));
      if (not jm->check_in_range(p1)) {
        LOG(3, "try again...");
        // exit(1);
      } else
        break;
    }

    Msg msg;
    msg.type = 2;
    msg.var.shared = p1;
    std::memcpy((void *)msg.var.shared, data, len);
    msg._size = len;
    // LOG(3, "shared, set size to: {}", msg._size);
    return msg;
  }

  static Msg cheap(Msg const &msg, std::shared_ptr<Jemalloc> &jm) {
    if (msg.type != 2) {
      throw 1;
    }
    Msg ret;
    ret.type = 22;
    ret.var.cheap = msg.var.shared;
    ret._size = msg._size;
    return ret;
  }

  static Msg rdkafka(std::unique_ptr<RdKafka::Message> &&rdkafka_msg) {
    Msg msg;
    msg.type = 1;
    msg.var.rdkafka_msg = rdkafka_msg.release();
    msg._size = msg.var.rdkafka_msg->len();
    return msg;
  }

  inline Msg(Msg &&x) {
    // LOG(3, "move ctor {} / {}   {} / {}", type, _size, x.type, x._size);
    using std::swap;
    swap(type, x.type);
    swap(var, x.var);
    swap(_size, x._size);
    // LOG(3, "move ctor {} / {}   {} / {}", type, _size, x.type, x._size);
  }

  inline void swap(Msg &y) {
    auto &x = *this;
    if (x.type != -1 && x.type != y.type) {
      LOG(1, "sorry, can not swap that");
      exit(1);
    }
    // LOG(3, "swap {} / {}   {} / {}", x.type, x._size, y.type, y._size);
    using std::swap;
    swap(x.type, y.type);
    swap(x.var, y.var);
    swap(x._size, y._size);
  }

  inline char const *data() const {
    switch (type) {
    case 1:
      return (char const *)var.rdkafka_msg->payload();
    case 0:
      return var.owned;
    case 2:
      return var.shared;
    case 22:
      return var.cheap;
    default:
      LOG(3, "error at type: {}", type);
      exit(1);
    }
    return "";
  }

  inline size_t size() const {
    switch (type) {
    case 1:
      return var.rdkafka_msg->len();
    case 0:
      return _size;
    case 2:
      return _size;
    case 22:
      return _size;
    default:
      LOG(3, "error at type: {}", type);
      exit(1);
    }
    return 0;
  }

  int type = -1;
  union Var {
    RdKafka::Message *rdkafka_msg;
    char const *owned;
    char const *shared;
    char const *cheap;
  } var;
  size_t _size = 0;

  inline ~Msg() {
    using std::unique_ptr;
    using std::shared_ptr;
    switch (type) {
    case 1:
      // var.rdkafka_msg.~unique_ptr<RdKafka::Message>();
      delete var.rdkafka_msg;
      break;
    case 0:
      // var.owned.~V0();
      delete var.owned;
      break;
    case 2:
      // TODO
      // control block is on separate allocation, but I try to dealloc on
      // worker..
      // var.shared.~shared_ptr<std::vector<char>>();
      delete var.shared;
      break;
    case 22:
      break;
    case -1:
      break;
    default:
      LOG(3, "error at type: {}", type);
      exit(1);
    }
  }

private:
};

} // namespace FileWriter
