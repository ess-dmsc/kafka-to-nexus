#include "Msg.h"

namespace KafkaW {

Msg::~Msg() {
  if (OnDelete) {
    OnDelete();
  }
}
}  //namespace KafkaW
