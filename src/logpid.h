#pragma once

#include "logger.h"
#include <thread>

void logpid(char const *fname) {
  FILE *f1 = fopen(fname, "wb");
  auto pidstr = fmt::format("{}", getpid());
  fwrite(pidstr.data(), pidstr.size(), 1, f1);
  fclose(f1);
  LOG(3, "logged pid {} to {}", pidstr, fname);
}

void sleep_ms(size_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}
