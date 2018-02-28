#pragma once

#include <cstdint>
#include <cstdlib>

namespace KafkaW {
// Want to expose this typedef also for users of this namespace
using uchar = unsigned char;

class Msg {
public:
  ~Msg();
  uchar *data();
  size_t size();
  void *MsgPtr = nullptr;
  char const *topicName();
  int64_t offset();
  int32_t partition();
  void *releaseMsgPtr();
};
}
