#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

int getpid_wrapper();

std::string gethostname_wrapper();

std::vector<char> readFileIntoVector(std::string const &FileName);

/// \todo Remove gulp when clang-tidy merge is done.
inline std::vector<char> gulp(std::string const &FileName) {
  return readFileIntoVector(FileName);
};
