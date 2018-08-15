#pragma once

#include "Msg.h"
#include <memory>

namespace KafkaW {

enum class PollStatusContent {
  Msg,
  Err,
  EOP,
  Empty,
};

class PollStatus {
public:
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
  PollStatusContent state = PollStatusContent::Err;
  std::unique_ptr<Msg> StoredMessage{nullptr};
};
}
