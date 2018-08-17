#include "Msg.h"

namespace KafkaW {

Msg::~Msg() {
  if (OnDelete) {
    OnDelete();
  }
}

std::pair<rd_kafka_timestamp_type_t, int64_t> Msg::timestamp() {
  std::pair<rd_kafka_timestamp_type_t, int64_t> TS;
  TS.second =
      rd_kafka_message_timestamp((rd_kafka_message_t *)DataPointer, &TS.first);
  return TS;
}

// <<<<<<< 700b7a6bc5fd158b6f69576c21dff1198517de2f
} // namespace KafkaW
// =======
// int32_t Msg::partition() { return ((rd_kafka_message_t *)MsgPtr)->partition;
// }

// int64_t Msg::offset() { return ((rd_kafka_message_t *)MsgPtr)->offset; }

// std::pair<rd_kafka_timestamp_type_t, int64_t> Msg::timestamp() {
//   std::pair<rd_kafka_timestamp_type_t, int64_t> TS;
//   TS.second =
//       rd_kafka_message_timestamp((rd_kafka_message_t *)MsgPtr, &TS.first);
//   return TS;
// }

// void *Msg::releaseMsgPtr() {
//   void *ptr = MsgPtr;
//   MsgPtr = nullptr;
//   return ptr;
// }
// }
// >>>>>>> Do not use class member for message timestamp but propagate in
// methods
