#include "logger.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

namespace trompeloeil {
template <>
void reporter<specialized>::send(severity s, char const *file,
                                 unsigned long line, const char *msg) {
  if (s == severity::fatal) {
    std::ostringstream os;
    if (line != 0U) {
      os << file << ':' << line << '\n';
    }
    throw expectation_violation(os.str() + msg);
  }
  ADD_FAILURE_AT(file, line) << msg;
}
} // namespace trompeloeil

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // do not use filewriterlogger during tests
  std::string ServiceID = "";
  std::string LogFile = "";
  auto GraylogURI = uri::URI();
  ::setUpLogging(spdlog::level::off, ServiceID, LogFile, GraylogURI);

  // set level for test logger
  spdlog::stdout_color_mt("testlogger")->set_level(spdlog::level::trace);
  return RUN_ALL_TESTS();
}
