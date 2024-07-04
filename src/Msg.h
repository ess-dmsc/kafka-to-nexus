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

/// \brief Structure for storing Kafka broker meta data.
struct MessageMetaData {
  [[nodiscard]] auto timestamp() const {
    return std::chrono::system_clock::time_point{Timestamp};
  }

  std::chrono::milliseconds Timestamp{0};
  RdKafka::MessageTimestamp::MessageTimestampType TimestampType{
      RdKafka::MessageTimestamp::MessageTimestampType::
          MSG_TIMESTAMP_NOT_AVAILABLE};
  int64_t Offset{0};
  int32_t Partition{0};
  std::string topic;
};

/// \brief A helper struct/class for storing Kafka messages that uses a unique
/// pointer for the data of the message.
struct Msg {
  Msg() = default;
  Msg(Msg &&Other) noexcept
      : DataPtr(std::move(Other.DataPtr)), Size(Other.Size),
        MetaData(Other.MetaData) {}
  Msg(char const *Data, size_t Bytes, MessageMetaData MessageInfo = {})
      : DataPtr(std::make_unique<char[]>(Bytes)), Size(Bytes),
        MetaData(MessageInfo) {
    std::memcpy(reinterpret_cast<void *>(DataPtr.get()), Data, Bytes);
  }
  Msg(uint8_t const *Data, size_t Bytes, MessageMetaData MessageInfo = {})
      : DataPtr(std::make_unique<char[]>(Bytes)), Size(Bytes),
        MetaData(MessageInfo) {
    std::memcpy(reinterpret_cast<void *>(DataPtr.get()), Data, Bytes);
  }
  Msg &operator=(Msg const &Other) {
    Size = Other.Size;
    MetaData = Other.MetaData;
    DataPtr = std::make_unique<char[]>(Size);
    std::memcpy(DataPtr.get(), Other.DataPtr.get(), Size);
    return *this;
  }

  uint8_t const *data() const {
    if (DataPtr == nullptr) {
      LOG_ERROR("Msg::data(): Data pointer is null.");
    }
    return reinterpret_cast<uint8_t const *>(DataPtr.get());
  }

  size_t size() const {
    if (DataPtr == nullptr) {
      LOG_ERROR("Msg::size(): Data pointer is null.");
    }
    return Size;
  }
  // Return value is const as it should/can not change.
  MessageMetaData const &getMetaData() const { return MetaData; }

protected:
  std::unique_ptr<char[]> DataPtr{nullptr};
  size_t Size{0};
  MessageMetaData MetaData;
};

} // namespace FileWriter
