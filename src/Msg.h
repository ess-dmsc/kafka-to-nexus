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

struct Msg {
  static Msg owned(char const *Data, size_t Bytes) {
    auto DataPtr = std::make_unique<char[]>(Bytes);
    std::memcpy(reinterpret_cast<void *>(DataPtr.get()), Data, Bytes);
    return {std::move(DataPtr), Bytes};
  }

  char const *data() const {
    if (DataPtr == nullptr) {
      LOG(Sev::Error, "error at type: {}", -1);
    }
    return DataPtr.get();
  }

  size_t size() const {
    if (DataPtr == nullptr) {
      LOG(Sev::Error, "error at type: {}", -1);
    }
    return Size;
  }

  std::unique_ptr<char[]> DataPtr{nullptr};
  size_t Size{0};
};

} // namespace FileWriter
