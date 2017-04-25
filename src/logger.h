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

void dwlog_inner(int level, char const *file, int line, char const *func,
                 std::string const &s1);

template <typename... TT>
void dwlog(int level, char const *fmt, char const *file, int line,
           char const *func, TT const &... args) {
  if (level > log_level)
    return;
  try {
    dwlog_inner(level, file, line, func, fmt::format(fmt, args...));
  }
  catch (fmt::FormatError &e) {
    dwlog_inner(level, file, line, func,
                fmt::format("ERROR in format: {}: {}", e.what(), fmt));
  }
}

void use_log_file(std::string fname);

void log_kafka_gelf_start(std::string broker, std::string topic);
void log_kafka_gelf_stop();

void fwd_graylog_logger_enable(std::string address);
