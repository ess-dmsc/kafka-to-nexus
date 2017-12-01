#pragma once

#include <array>
#include <memory>
#include <string>
#include <vector>

struct MPIChild {
  using ptr = std::unique_ptr<MPIChild>;
  std::vector<char> cmd;
  std::string config;
};
