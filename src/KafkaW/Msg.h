#pragma once

#include <cstdint>
#include <cstdlib>
#include <functional>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

class Msg {
public:
  Msg() = default;
  Msg(std::uint8_t const *Pointer, size_t Size, std::function<void()> DataDeleter) : DataPointer(Pointer), DataSize(Size), OnDelete(DataDeleter) {}
  ~Msg();
  std::uint8_t const *data() const {return DataPointer;};
  size_t size() const {return DataSize;};
private:
  unsigned char const *DataPointer{nullptr};
  size_t DataSize{0};
  std::function<void()> OnDelete;
};
}
