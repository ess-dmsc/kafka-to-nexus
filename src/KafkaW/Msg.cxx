#include "Msg.h"

#include <librdkafka/rdkafka.h>

namespace KafkaW {

Msg::~Msg() { rd_kafka_message_destroy((rd_kafka_message_t *)MsgPtr); }

uchar *Msg::data() { return (uchar *)((rd_kafka_message_t *)MsgPtr)->payload; }

size_t Msg::size() { return ((rd_kafka_message_t *)MsgPtr)->len; }

char const *Msg::topicName() {
  return rd_kafka_topic_name(((rd_kafka_message_t *)MsgPtr)->rkt);
}

int32_t Msg::partition() { return ((rd_kafka_message_t *)MsgPtr)->partition; }

int64_t Msg::offset() { return ((rd_kafka_message_t *)MsgPtr)->offset; }
}
