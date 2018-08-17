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

} // namespace KafkaW
