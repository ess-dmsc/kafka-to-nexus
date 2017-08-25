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

  static Msg rdkafka(std::unique_ptr<RdKafka::Message> &&rdkafka_msg) {
    Msg msg;
    msg.type = 1;
    msg.var.rdkafka_msg = std::move(rdkafka_msg);
    return msg;
  }

  inline Msg(Msg &&msg) {
    type = msg.type;
    msg.type = 0;
    switch (type) {
    case 1:
      var.rdkafka_msg = std::move(msg.var.rdkafka_msg);
      break;
    case 0:
      var.owned = std::move(msg.var.owned);
      break;
    }
  }

  inline char const *data() const {
    switch (type) {
    case 1:
      return (char const *)var.rdkafka_msg->payload();
    case 0:
      return var.owned.data.data();
    }
    return "";
  }

  inline size_t size() const {
    switch (type) {
    case 1:
      return var.rdkafka_msg->len();
    case 0:
      return var.owned.data.size();
    }
    return 0;
  }

  int type;
  union Var {
    Var() {}
    ~Var() {}
    std::unique_ptr<RdKafka::Message> rdkafka_msg{nullptr};
    struct V0 {
      std::vector<char> data;
    } owned;
  } var;

  inline ~Msg() {
    using std::unique_ptr;
    switch (type) {
    case 1:
      var.rdkafka_msg.~unique_ptr<RdKafka::Message>();
      break;
    case 0:
      var.owned.~V0();
      break;
    }
  }

private:
  Msg() {}
};

} // namespace FileWriter
