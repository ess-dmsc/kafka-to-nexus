#pragma once

#include <cstdint>
#include <cstdlib>
#include <librdkafka/rdkafka.h>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

class Msg {
public:
  Msg() = default;
  Msg(rd_kafka_message_t *Pointer) : MsgPtr(Pointer) {}
  ~Msg();
  uchar *data();
  size_t size();
  char const *topicName();
  int64_t offset();
  int32_t partition();
  void *releaseMsgPtr();
private:
  rd_kafka_message_t *MsgPtr = nullptr;
};
}
