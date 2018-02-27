#pragma once

#include "Msg.h"
#include <memory>

namespace KafkaW {

class PollStatus {
public:
  static PollStatus Ok();
  static PollStatus Err();
  static PollStatus EOP();
  static PollStatus Empty();
  static PollStatus newWithMsg(std::unique_ptr<Msg> x);
  PollStatus(PollStatus &&);
  PollStatus &operator=(PollStatus &&);
  ~PollStatus();
  void reset();
  PollStatus();
  bool isOk();
  bool isErr();
  bool isEOP();
  bool isEmpty();
  std::unique_ptr<Msg> isMsg();

private:
  int state = -1;
  void *data = nullptr;
};
}
