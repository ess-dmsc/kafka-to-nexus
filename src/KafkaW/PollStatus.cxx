#include "PollStatus.h"

namespace KafkaW {

PollStatus::~PollStatus() { reset(); }

PollStatus PollStatus::Okkk() {
  PollStatus ret;
  ret.state = PollStatusContent::Msg;
  return ret;
}

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
  ret.data = Msg.release();
  return ret;
}

PollStatus::PollStatus(PollStatus &&x)
    : state(std::move(x.state)), data(std::move(x.data)) {}

PollStatus &PollStatus::operator=(PollStatus &&x) {
  reset();
  std::swap(state, x.state);
  std::swap(data, x.data);
  return *this;
}

void PollStatus::reset() {
  if (state == PollStatusContent::Msg) {
    if (auto x = (Msg *)data) {
      delete x;
    }
  }
  state = PollStatusContent::Empty;
  data = nullptr;
}

PollStatus::PollStatus() {}

bool PollStatus::isOk() { return state == PollStatusContent::Msg; }

bool PollStatus::isErr() { return state == PollStatusContent::Err; }

bool PollStatus::isEOP() { return state == PollStatusContent::EOP; }

bool PollStatus::isEmpty() { return state == PollStatusContent::Empty; }

std::unique_ptr<Msg> PollStatus::isMsg() {
  if (state == PollStatusContent::Msg) {
    std::unique_ptr<Msg> ret((Msg *)data);
    data = nullptr;
    state = PollStatusContent::Empty;
    return ret;
  }
  return nullptr;
}
}
