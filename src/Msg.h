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

namespace FileWriter {

enum class MsgType : int {
  Invalid = -1,
  Owned = 0,
};

struct Msg {
  static Msg owned(char const *Data, size_t Bytes) {
    auto DataPtr = std::make_unique<char[]>(Bytes);
    std::memcpy(reinterpret_cast<void *>(DataPtr.get()), Data, Bytes);
    return {MsgType::Owned, std::move(DataPtr), Bytes};
  }

  /// \todo Delete this function when the last clang-tidy PR is merged.
  static Msg shared(char const *Data, size_t Bytes) {
    return Msg::owned(Data, Bytes);
  }

  char const *data() const {
    switch (Type) {
    case MsgType::Owned:
      return DataPtr.get();
    default:
      LOG(Sev::Error, "error at type: {}", static_cast<int>(Type));
    }
    return nullptr;
  }

  size_t size() const {
    switch (Type) {
    case MsgType::Owned:
      return Size;
    default:
      LOG(Sev::Error, "error at type: {}", static_cast<int>(Type));
    }
    return 0;
  }

  MsgType Type{MsgType::Invalid};
  std::unique_ptr<char[]> DataPtr;
  size_t Size{0};
};

} // namespace FileWriter
