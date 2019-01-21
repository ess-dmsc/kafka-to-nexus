#pragma once

#include "KafkaW/ConsumerMessage.h"
#include "logger.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <memory>
#include <vector>
#include <spdlog/spdlog.h>

namespace FileWriter {

enum class MsgType : int {
  Invalid = -1,
  Owned = 0,
  RdKafka = 1,
  Shared = 2,
  KafkaW = 3,
  Cheap = 22,
};

class Msg {
public:
  Msg() : type(MsgType::Invalid) {}

  static Msg owned(char const *data, size_t len) {
    Msg msg;
    msg.type = MsgType::Owned;
    msg.var.owned = new char[len];
    std::memcpy((void *)msg.var.owned, data, len);
    msg._size = len;
    return msg;
  }

  static Msg shared(char const *data, size_t len) {
    char *p1 = new char[len];

    Msg msg;
    msg.type = MsgType::Shared;
    msg.var.shared = p1;
    std::memcpy((void *)msg.var.shared, data, len);
    msg._size = len;
    return msg;
  }

  static Msg cheap(Msg const &msg) {
    Msg ret;
    if (msg.type != MsgType::Shared) {
      LOG(spdlog::level::critical, "msg.type != MsgType::Shared");
      return ret;
    }
    ret.type = MsgType::Cheap;
    ret.var.cheap = msg.var.shared;
    ret._size = msg._size;
    return ret;
  }

  // Can be removed when we use the KafkaW wrapper everywhere.

  static Msg rdkafka(std::unique_ptr<RdKafka::Message> &&rdkafka_msg) {
    Msg msg;
    msg.type = MsgType::RdKafka;
    msg.var.rdkafka_msg = rdkafka_msg.release();
    msg._size = msg.var.rdkafka_msg->len();
    return msg;
  }

  static Msg fromKafkaW(std::unique_ptr<KafkaW::ConsumerMessage> &&KafkaWMsg) {
    Msg msg;
    msg.type = MsgType::KafkaW;
    msg.var.kafkaw_msg = KafkaWMsg.release();
    msg._size = 0;
    return msg;
  }

  inline Msg(Msg &&x) {
    using std::swap;
    swap(type, x.type);
    swap(var, x.var);
    swap(_size, x._size);
  }

  inline void swap(Msg &y) {
    if (type != MsgType::Invalid && type != y.type) {
      LOG(spdlog::level::critical, "sorry, can not swap that");
    }
    using std::swap;
    swap(type, y.type);
    swap(var, y.var);
    swap(_size, y._size);
  }

  inline char const *data() const {
    switch (type) {
    case MsgType::RdKafka:
      return static_cast<char const *>(var.rdkafka_msg->payload());
    case MsgType::KafkaW:
      return reinterpret_cast<char const *>(var.kafkaw_msg->getData());
    case MsgType::Owned:
      return var.owned;
    case MsgType::Shared:
      return var.shared;
    case MsgType::Cheap:
      return var.cheap;
    default:
      LOG(spdlog::level::err, "error at type: {}", std::to_string(static_cast<int>(type)));
    }
    return "";
  }

  inline size_t size() const {
    switch (type) {
    case MsgType::RdKafka:
      return var.rdkafka_msg->len();
    case MsgType::KafkaW:
      return var.kafkaw_msg->getSize();
    case MsgType::Owned:
      return _size;
    case MsgType::Shared:
      return _size;
    case MsgType::Cheap:
      return _size;
    default:
      LOG(spdlog::level::err, "error at type: {}", static_cast<int>(type));
    }
    return 0;
  }

  MsgType type = MsgType::Invalid;
  union Var {
    RdKafka::Message *rdkafka_msg;
    KafkaW::ConsumerMessage *kafkaw_msg;
    char const *owned;
    char const *shared;
    char const *cheap;
  } var;
  size_t _size = 0;

  /// TODO: Is this the correct usage of delete?
  inline ~Msg() {
    switch (type) {
    case MsgType::RdKafka:
      delete var.rdkafka_msg;
      break;
    case MsgType::KafkaW:
      delete var.kafkaw_msg;
      break;
    case MsgType::Owned:
      delete[] var.owned;
      break;
    case MsgType::Shared:
      delete[] var.shared;
      break;
    case MsgType::Cheap:
      break;
    case MsgType::Invalid:
      break;
    default:
      LOG(spdlog::level::err, "unhandled type: {}", static_cast<int>(type));
    }
  }
};

} // namespace FileWriter
