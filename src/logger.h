#pragma once

#include <fmt/format.h>
#include <string>

#ifdef _MSC_VER

#define LOG(level, fmt, ...)                                                   \
  { dwlog(level, fmt, __FILE__, __LINE__, __FUNCSIG__, __VA_ARGS__); }

#else

#define LOG(level, fmt, args...)                                               \
  { dwlog(level, fmt, __FILE__, __LINE__, __PRETTY_FUNCTION__, ##args); }

#endif

extern int log_level;

//These severity level correspond to the RFC3164 (syslog) severity levels
enum class Sev : int {
  Emergency = 0,  //Do not use, reserved for system errors
  Alert = 1,      //Do not use, reserved for system errors
  Crit = 2,       //Critical, i.e. wake me up in the middle of the night
  Err = 3,        //Error, I should fix this as soon as possible
  Warn = 4,       //Could be an indication of a problem that needs fixing
  Note = 5,       //Notice, potentially important events that might need special treatment
  Info = 6,       //Informational, general run time info for checking the state of the application
  Dbg = 7,      //Debug, give me a flood of information
};

void dwlog_inner(int level, char const *file, int line, char const *func,
                 std::string const &s1);

template <typename... TT>
void dwlog(Sev level, char const *fmt, char const *file, int line,
           char const *func, TT const &... args) {
  if (static_cast<int>(level) > log_level)
    return;
  try {
    dwlog_inner(static_cast<int>(level), file, line, func, fmt::format(fmt, args...));
  } catch (fmt::FormatError &e) {
    dwlog_inner(static_cast<int>(level), file, line, func,
                fmt::format("ERROR in format: {}: {}", e.what(), fmt));
  }
}

void use_log_file(std::string fname);

void log_kafka_gelf_start(std::string broker, std::string topic);
void log_kafka_gelf_stop();

void fwd_graylog_logger_enable(std::string address);
