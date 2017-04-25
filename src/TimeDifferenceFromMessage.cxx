#include "TimeDifferenceFromMessage.h"

namespace BrightnESS {
namespace FileWriter {

TimeDifferenceFromMessage_DT::TimeDifferenceFromMessage_DT(
    const std::string &sourcename, int64_t dt)
    : sourcename(sourcename), dt(dt) {}

const TimeDifferenceFromMessage_DT TimeDifferenceFromMessage_DT::OK() {
  TimeDifferenceFromMessage_DT ret("ok", 0);
  return ret;
}

const TimeDifferenceFromMessage_DT TimeDifferenceFromMessage_DT::ERR() {
  TimeDifferenceFromMessage_DT ret("", 0);
  return ret;
}

const TimeDifferenceFromMessage_DT TimeDifferenceFromMessage_DT::BOP() {
  TimeDifferenceFromMessage_DT ret("eof", 0);
  return ret;
}
} // namespace FileWriter
} // namespace BrightnESS
