#pragma once

#include <cstdint>
#include <cstdlib>
// <<<<<<< 700b7a6bc5fd158b6f69576c21dff1198517de2f
#include <functional>
#include <librdkafka/rdkafka.h>
// =======
// #include <librdkafka/rdkafka.h>
// #include <map>
// >>>>>>> Do not use class member for message timestamp but propagate in
// methods

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

class Msg {
public:
  Msg() = default;
  Msg(std::uint8_t const *Pointer, size_t Size,
      std::function<void()> DataDeleter)
      : DataPointer(Pointer), DataSize(Size), OnDelete(std::move(DataDeleter)) {
  }
  Msg(std::uint8_t const *Pointer, size_t Size,
      std::function<void()> DataDeleter, std::int64_t Offset)
      : DataPointer(Pointer), DataSize(Size), OnDelete(std::move(DataDeleter)),
        MessageOffset(Offset) {}
  ~Msg();
  // <<<<<<< 700b7a6bc5fd158b6f69576c21dff1198517de2f
  std::uint8_t const *data() const { return DataPointer; };
  size_t size() const { return DataSize; };
  std::uint64_t getMessageOffset() const { return MessageOffset; };
  std::pair<rd_kafka_timestamp_type_t, int64_t> timestamp();

private:
  unsigned char const *DataPointer{nullptr};
  size_t DataSize{0};
  std::function<void()> OnDelete;
  std::int64_t MessageOffset{0};
  // =======
  //   uchar *data();
  //   size_t size();
  //   void *MsgPtr = nullptr;
  //   char const *topicName();
  //   int64_t offset();
  //   int32_t partition();
  //   std::pair<rd_kafka_timestamp_type_t, int64_t> timestamp();
  //   void *releaseMsgPtr();
  // >>>>>>> Do not use class member for message timestamp but propagate in
  // methods
};
} // namespace KafkaW
