// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "logger.h"
#include <chrono>
#include <librdkafka/rdkafkacpp.h>
#include <memory>

namespace FileWriter {

struct MessageMetaData {
  std::chrono::milliseconds Timestamp{0};
  RdKafka::MessageTimestamp::MessageTimestampType TimestampType{
      RdKafka::MessageTimestamp::MessageTimestampType::
          MSG_TIMESTAMP_NOT_AVAILABLE};
  int64_t Offset{0};
  int32_t Partition{0};
};

struct Msg {
  Msg() = default;
  Msg(Msg &&Other) noexcept
      : DataPtr(std::move(Other.DataPtr)), Size(Other.Size),
        MetaData(Other.MetaData) {}
  Msg(char const *Data, size_t Bytes)
      : DataPtr(std::make_unique<char[]>(Bytes)), Size(Bytes) {
    std::memcpy(reinterpret_cast<void *>(DataPtr.get()), Data, Bytes);
  }
  Msg &operator=(Msg const &Other) {
    Size = Other.Size;
    MetaData = Other.MetaData;
    DataPtr = std::make_unique<char[]>(Size);
    std::memcpy(DataPtr.get(), Other.DataPtr.get(), Size);
    return *this;
  }

  char const *data() const {
    if (DataPtr == nullptr) {
      getLogger()->error("error at type: {}", -1);
    }
    return DataPtr.get();
  }

  size_t size() const {
    if (DataPtr == nullptr) {
      getLogger()->error("error at type: {}", -1);
    }
    return Size;
  }

  std::unique_ptr<char[]> DataPtr{nullptr};
  size_t Size{0};
  MessageMetaData MetaData;
};

} // namespace FileWriter
