#pragma once

#include <cstddef>
#include <cstdint>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

namespace FileWriter {

class Msg {
public:
  static Msg owned(char const *data, size_t len) {
    Msg msg;
    msg.type = 0;
    msg.var.owned.data = std::vector<char>(data, data + len);
    return msg;
  }

  static Msg shared(char const *data, size_t len) {
    Msg msg;
    msg.type = 2;
    msg.var.shared = std::make_shared<std::vector<char>>(data, data + len);
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
    msg.var.rdkafka_msg = std::move(rdkafka_msg);
    return msg;
  }

  inline Msg(Msg &&msg) {
    type = msg.type;
    msg.type = -1;
    switch (type) {
    case 1:
      var.rdkafka_msg = std::unique_ptr<RdKafka::Message>(nullptr);
      var.rdkafka_msg = std::move(msg.var.rdkafka_msg);
      break;
    case 0:
      var.owned = V0();
      var.owned = std::move(msg.var.owned);
      break;
    case 2:
      var.shared = msg.var.shared;
      msg.var.shared.reset();
      break;
    }
  }

  inline char const *data() const {
    switch (type) {
    case 1:
      return (char const *)var.rdkafka_msg->payload();
    case 0:
      return var.owned.data.data();
    case 2:
      return var.shared->data();
    }
    return "";
  }

  inline size_t size() const {
    switch (type) {
    case 1:
      return var.rdkafka_msg->len();
    case 0:
      return var.owned.data.size();
    case 2:
      return var.shared->size();
    }
    return 0;
  }

  struct V0 {
    std::vector<char> data;
  };
  int type;
  union Var {
    Var() : owned() {}
    ~Var() {}
    std::unique_ptr<RdKafka::Message> rdkafka_msg{nullptr};
    V0 owned;
    std::shared_ptr<std::vector<char>> shared;
  } var;

  inline ~Msg() {
    using std::unique_ptr;
    using std::shared_ptr;
    switch (type) {
    case 1:
      var.rdkafka_msg.~unique_ptr<RdKafka::Message>();
      break;
    case 0:
      var.owned.~V0();
      break;
    case 2:
      var.shared.~shared_ptr<std::vector<char>>();
      break;
    }
  }

private:
  inline Msg() {}
};

} // namespace FileWriter
