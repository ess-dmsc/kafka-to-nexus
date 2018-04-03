#pragma once

#include "Msg.h"
#include <string>

namespace FileWriter {

class TimeDifferenceFromMessage {
public:
  static const TimeDifferenceFromMessage OK();
  static const TimeDifferenceFromMessage ERR();
  static const TimeDifferenceFromMessage BOP(); // aka beginning of partition
  inline bool is_OK() { return sourcename != ""; }
  inline bool is_ERR() { return sourcename == ""; }
  inline bool is_BOP() { return sourcename == "eof"; }
  std::string sourcename;
  int64_t dt;
  TimeDifferenceFromMessage(const std::string &sourcename, int64_t dt);
};

} // namespace FileWriter
