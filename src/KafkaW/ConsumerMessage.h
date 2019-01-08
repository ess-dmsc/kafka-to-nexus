#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>
#include <librdkafka/rdkafka.h>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

enum class PollStatus {
  Msg,
  Err,
  EOP,
  Empty,
};

class ConsumerMessage {
public:
  ConsumerMessage() = default;
  ConsumerMessage(std::uint8_t const *Pointer, size_t Size,
                  std::function<void()> DataDeleter)
      : DataPointer(Pointer), DataSize(Size), OnDelete(std::move(DataDeleter)) {
  }
  ConsumerMessage(std::uint8_t const *Pointer, size_t Size,
                  std::function<void()> DataDeleter, std::int64_t Offset)
      : DataPointer(Pointer), DataSize(Size), OnDelete(std::move(DataDeleter)),
        MessageOffset(Offset) {}

  explicit ConsumerMessage(PollStatus Status) : Status(Status) {}
  ~ConsumerMessage() {
    if (OnDelete) {
      OnDelete();
    }
  };
  std::uint8_t const *getData() const { return DataPointer; };
  size_t getSize() const { return DataSize; };
  std::int64_t getMessageOffset() const { return MessageOffset; };
  PollStatus getStatus() const { return Status; };
  std::pair<rd_kafka_timestamp_type_t, int64_t> getTimestamp() {
    std::pair<rd_kafka_timestamp_type_t, int64_t> TS;
    TS.second =
        rd_kafka_message_timestamp((rd_kafka_message_t *)DataPointer, &TS.first);
    return TS;
  }

private:
  unsigned char const *DataPointer{nullptr};
  size_t DataSize{0};
  std::function<void()> OnDelete;
  PollStatus Status{PollStatus::Msg};
  std::int64_t MessageOffset{0};
};
} // namespace KafkaW
