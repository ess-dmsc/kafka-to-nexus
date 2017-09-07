#pragma once

#include "logger.h"
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

  static Msg shared(char const *data, size_t len) {
    Msg msg;
    msg.type = 2;
    msg.var.shared =
        new std::shared_ptr<char>(new char[len], std::default_delete<char[]>());
    std::memcpy((*msg.var.shared).get(), data, len);
    msg._size = len;
    return msg;
  }

  static Msg shared(Msg const &msg) {
    if (msg.type != 2) {
      throw 1;
    }
    Msg ret;
    ret.type = 2;
    ret.var.shared = msg.var.shared;
    return ret;
  }

  static Msg rdkafka(std::unique_ptr<RdKafka::Message> &&rdkafka_msg) {
    Msg msg;
    msg.type = 1;
    msg.var.rdkafka_msg = rdkafka_msg.release();
    return msg;
  }

  inline Msg(Msg &&x) {
    using std::swap;
    swap(type, x.type);
    swap(var, x.var);
  }

  inline void swap(Msg &x, Msg &y) {
    if (x.type != -1 && x.type != y.type) {
      LOG(1, "sorry, can not swap that");
      exit(1);
    }
    using std::swap;
    swap(x.type, y.type);
    swap(x.var, y.var);
  }

  inline char const *data() const {
    switch (type) {
    case 1:
      return (char const *)var.rdkafka_msg->payload();
    case 0:
      return var.owned;
    case 2:
      return (*var.shared).get();
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
    }
    return 0;
  }

  int type = -1;
  union Var {
    RdKafka::Message *rdkafka_msg;
    char const *owned;
    std::shared_ptr<char> *shared;
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
      // var.shared.~shared_ptr<std::vector<char>>();
      delete var.shared;
      break;
    case -1:
      break;
    }
  }

private:
};

} // namespace FileWriter
