#pragma once

#include "Alloc.h"
#include "KafkaW/Msg.h"
#include "logger.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>

namespace FileWriter {

enum class MsgType : int {
  Invalid = -1,
  Owned = 0,
  RdKafka = 1,
  Shared = 2,
  RdKafkaCPtr = 3,
  Cheap = 22,
};

class Msg {
public:
  Msg() { type = MsgType::Invalid; }

  static Msg owned(char const *data, size_t len) {
    Msg msg;
    msg.type = MsgType::Owned;
    msg.var.owned = new char[len];
    std::memcpy((void *)msg.var.owned, data, len);
    msg._size = len;
    return msg;
  }

  static Msg shared(char const *data, size_t len, std::shared_ptr<Alloc> &jm) {
    jm->use_this();
    Alloc::tcache_flush();
    char *p1;
    while (true) {
      p1 = (char *)jm->alloc(len * sizeof(char));
      if (not jm->check_in_range(p1)) {
        LOG(Sev::Error, "try again...");
        // exit(1);
      } else
        break;
    }
    jm->use_default();
    Alloc::tcache_flush();

    Msg msg;
    msg.type = MsgType::Shared;
    msg.var.shared = p1;
    std::memcpy((void *)msg.var.shared, data, len);
    msg._size = len;
    return msg;
  }

  static Msg cheap(Msg const &msg, std::shared_ptr<Alloc> &jm) {
    if (msg.type != MsgType::Shared) {
      throw "msg.type != MsgType::Shared";
    }
    Msg ret;
    ret.type = MsgType::Cheap;
    ret.var.cheap = msg.var.shared;
    ret._size = msg._size;
    return ret;
  }

  static Msg rdkafka(std::unique_ptr<RdKafka::Message> &&rdkafka_msg) {
    Msg msg;
    msg.type = MsgType::RdKafka;
    msg.var.rdkafka_msg = rdkafka_msg.release();
    msg._size = msg.var.rdkafka_msg->len();
    return msg;
  }

  static Msg fromKafkaW(std::unique_ptr<KafkaW::Msg> KafkaWMsg) {
    Msg msg;
    msg.type = MsgType::RdKafkaCPtr;
    msg.var.rdkafka_msg_c_ptr =
        static_cast<rd_kafka_message_t *>(KafkaWMsg->releaseMsgPtr());
    msg._size = msg.var.rdkafka_msg_c_ptr->len;
    return msg;
  }

  inline Msg(Msg &&x) {
    using std::swap;
    swap(type, x.type);
    swap(var, x.var);
    swap(_size, x._size);
  }

  inline void swap(Msg &y) {
    auto &x = *this;
    if (x.type != MsgType::Invalid && x.type != y.type) {
      LOG(Sev::Critical, "sorry, can not swap that");
    }
    using std::swap;
    swap(x.type, y.type);
    swap(x.var, y.var);
    swap(x._size, y._size);
  }

  inline char const *data() const {
    switch (type) {
    case MsgType::RdKafka:
      return (char const *)var.rdkafka_msg->payload();
    case MsgType::RdKafkaCPtr:
      return (char const *)var.rdkafka_msg_c_ptr->payload;
    case MsgType::Owned:
      return var.owned;
    case MsgType::Shared:
      return var.shared;
    case MsgType::Cheap:
      return var.cheap;
    default:
      LOG(Sev::Error, "error at type: {}", static_cast<int>(type));
      exit(1);
    }
    return "";
  }

  inline size_t size() const {
    switch (type) {
    case MsgType::RdKafka:
      return var.rdkafka_msg->len();
    case MsgType::RdKafkaCPtr:
      return var.rdkafka_msg_c_ptr->len;
    case MsgType::Owned:
      return _size;
    case MsgType::Shared:
      return _size;
    case MsgType::Cheap:
      return _size;
    default:
      LOG(Sev::Error, "error at type: {}", static_cast<int>(type));
      exit(1);
    }
    return 0;
  }

  MsgType type = MsgType::Invalid;
  union Var {
    RdKafka::Message *rdkafka_msg;
    rd_kafka_message_t *rdkafka_msg_c_ptr;
    char const *owned;
    char const *shared;
    char const *cheap;
  } var;
  size_t _size = 0;

  inline ~Msg() {
    switch (type) {
    case MsgType::RdKafka:
      // var.rdkafka_msg.~unique_ptr<RdKafka::Message>();
      delete var.rdkafka_msg;
      break;
    case MsgType::RdKafkaCPtr:
      rd_kafka_message_destroy(var.rdkafka_msg_c_ptr);
      break;
    case MsgType::Owned:
      // var.owned.~V0();
      delete var.owned;
      break;
    case MsgType::Shared:
      // var.shared.~shared_ptr<std::vector<char>>();
      delete var.shared;
      break;
    case MsgType::Cheap:
      break;
    case MsgType::Invalid:
      break;
    default:
      LOG(Sev::Error, "unhandled type: {}", static_cast<int>(type));
      exit(1);
    }
  }
};

} // namespace FileWriter
