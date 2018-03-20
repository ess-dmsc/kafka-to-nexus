#pragma once

#include "Msg.h"
#include <string>

namespace FileWriter {

class TimeDifferenceFromMessage_DT {
public:
  static const TimeDifferenceFromMessage_DT OK();
  static const TimeDifferenceFromMessage_DT ERR();
  static const TimeDifferenceFromMessage_DT BOP(); // aka beginning of partition
  inline bool is_OK() { return sourcename != ""; }
  inline bool is_ERR() { return sourcename == ""; }
  inline bool is_BOP() { return sourcename == "eof"; }
  std::string sourcename;
  int64_t dt;
  TimeDifferenceFromMessage_DT(const std::string &sourcename, int64_t dt);
};

} // namespace FileWriter
