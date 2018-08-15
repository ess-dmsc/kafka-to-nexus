#include "Msg.h"

namespace KafkaW {

Msg::~Msg() {
  if (MsgPtr) {
    rd_kafka_message_destroy(MsgPtr);
  }
}

uchar *Msg::data() { return (uchar *)MsgPtr->payload; }

size_t Msg::size() { return MsgPtr->len; }

char const *Msg::topicName() {
  return rd_kafka_topic_name(MsgPtr->rkt);
}

int32_t Msg::partition() { return MsgPtr->partition; }

int64_t Msg::offset() { return MsgPtr->offset; }

void *Msg::releaseMsgPtr() {
  void *ptr = MsgPtr;
  MsgPtr = nullptr;
  return ptr;
}
}
