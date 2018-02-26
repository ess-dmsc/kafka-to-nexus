#pragma once

#include <cstdint>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

class Msg {
public:
  ~Msg();
  uchar *data();
  uint32_t size();
  void *MsgPtr;
  char const *topic_name();
  int32_t offset();
  int32_t partition();
};
}
