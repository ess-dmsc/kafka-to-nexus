#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

enum class PollStatus {
  Msg,
  Err,
  EOP,
  Empty,
};

class Msg {
public:
  Msg() = default;
  Msg(std::uint8_t const *Pointer, size_t Size,
      std::function<void()> DataDeleter, PollStatus Status)
      : DataPointer(Pointer), DataSize(Size), OnDelete(std::move(DataDeleter)),
        Status(Status) {}
  Msg(std::uint8_t const *Pointer, size_t Size,
      std::function<void()> DataDeleter, PollStatus Status, std::int64_t Offset)
      : DataPointer(Pointer), DataSize(Size), OnDelete(std::move(DataDeleter)),
        Status(Status), MessageOffset(Offset) {}

  Msg(PollStatus Status) : Status(Status) {}
  ~Msg() {
    if (OnDelete) {
      OnDelete();
    }
  };
  std::uint8_t const *data() const { return DataPointer; };
  size_t size() const { return DataSize; };
  std::uint64_t getMessageOffset() const { return MessageOffset; };
  PollStatus getStatus() const { return Status; };

private:
  unsigned char const *DataPointer{nullptr};
  size_t DataSize{0};
  std::function<void()> OnDelete;
  PollStatus Status{PollStatus::Empty};
  std::int64_t MessageOffset{0};
};
} // namespace KafkaW
