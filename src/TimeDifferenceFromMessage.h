#pragma once
#include <string>

namespace BrightnESS {
namespace FileWriter {

class TimeDifferenceFromMessage_DT {
public:
  static const TimeDifferenceFromMessage_DT OK();
  static const TimeDifferenceFromMessage_DT ERR();
  static const TimeDifferenceFromMessage_DT BOP(); // aka beginning of partition
  inline bool is_OK() { return sourcename != ""; }
  inline bool is_ERR() { return sourcename == ""; }
  inline bool is_BOP() { return sourcename == "eof"; }
std::string const & sourcename;
int64_t dt;
TimeDifferenceFromMessage_DT(const std::string & sourcename, int64_t dt);
};

class TimeDifferenceFromMessage {
public:
using DT = TimeDifferenceFromMessage_DT;
/// Given a message, returns the sourcename and the time difference
/// `dt = tm - t0` (milliseconds) between the message `tm` and the
/// time at which `sourcename` would like to start to consume data.
virtual
DT time_difference_from_message(char * msg_data, int msg_size) = 0;
};

}
}
