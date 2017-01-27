#pragma once

#include <chrono>

using milliseconds = std::chrono::milliseconds;

constexpr milliseconds operator "" _ms(const unsigned long long int value) {
  return milliseconds(value);
}
