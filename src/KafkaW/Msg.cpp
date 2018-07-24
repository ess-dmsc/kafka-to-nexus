#include "Msg.h"
#include <librdkafka/rdkafka.h>

namespace KafkaW {

Msg::~Msg() {
  if (OnDelete) {
    OnDelete();
  }
}

int64_t Msg::timestamp() {
  rd_kafka_timestamp_type_t TSType;
  // Ignore info about timestamp type
  return rd_kafka_message_timestamp((rd_kafka_message_t *)DataPointer, &TSType);
}

} // namespace KafkaW
