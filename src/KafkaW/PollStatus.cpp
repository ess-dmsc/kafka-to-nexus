#include "PollStatus.h"

namespace KafkaW {

PollStatus::~PollStatus() { reset(); }

PollStatus PollStatus::Err() {
  PollStatus ret;
  ret.state = PollStatusContent::Err;
  return ret;
}

PollStatus PollStatus::EOP() {
  PollStatus ret;
  ret.state = PollStatusContent::EOP;
  return ret;
}

PollStatus PollStatus::Empty() {
  PollStatus ret;
  ret.state = PollStatusContent::Empty;
  return ret;
}

PollStatus PollStatus::newWithMsg(std::unique_ptr<Msg> Msg) {
  PollStatus ret;
  ret.state = PollStatusContent::Msg;
  ret.StoredMessage = std::move(Msg);
  return ret;
}

PollStatus::PollStatus(PollStatus &&x)
    : state(std::move(x.state)), StoredMessage(std::move(x.StoredMessage)) {}

PollStatus &PollStatus::operator=(PollStatus &&x) {
  reset();
  std::swap(state, x.state);
  std::swap(StoredMessage, x.StoredMessage);
  return *this;
}

void PollStatus::reset() {
  if (state == PollStatusContent::Msg) {
    StoredMessage.reset();
  }
  state = PollStatusContent::Empty;
}

PollStatus::PollStatus() {}

bool PollStatus::isOk() { return state == PollStatusContent::Msg; }

bool PollStatus::isErr() { return state == PollStatusContent::Err; }

bool PollStatus::isEOP() { return state == PollStatusContent::EOP; }

bool PollStatus::isEmpty() { return state == PollStatusContent::Empty; }

std::unique_ptr<Msg> PollStatus::isMsg() {
  if (state == PollStatusContent::Msg) {
    state = PollStatusContent::Empty;
    return std::move(StoredMessage);
  }
  return nullptr;
}
}
