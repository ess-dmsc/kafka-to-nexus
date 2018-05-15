#include "TimeDifferenceFromMessage.h"

namespace FileWriter {

TimeDifferenceFromMessage::TimeDifferenceFromMessage(
    const std::string &sourcename, int64_t dt)
    : sourcename(sourcename), dt(dt) {}

const TimeDifferenceFromMessage TimeDifferenceFromMessage::OK() {
  TimeDifferenceFromMessage ret("ok", 0);
  return ret;
}

const TimeDifferenceFromMessage TimeDifferenceFromMessage::ERR() {
  TimeDifferenceFromMessage ret("", 0);
  return ret;
}

const TimeDifferenceFromMessage TimeDifferenceFromMessage::BOP() {
  TimeDifferenceFromMessage ret("eof", 0);
  return ret;
}
} // namespace FileWriter
