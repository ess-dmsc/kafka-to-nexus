#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

void sleep_ms(size_t ms);

uint64_t getpid_wrapper();

// note: this implementation does not disable this overload for array types
template <typename T, typename... TX>
std::unique_ptr<T> make_unique(TX &&... tx) {
  return std::unique_ptr<T>(new T(std::forward<TX>(tx)...));
}

std::vector<char> gulp(std::string fname);

std::vector<char> binary_to_hex(char const *data, uint32_t len);

std::vector<std::string> split(std::string const &input, std::string token);

template <typename T>
std::pair<short int, T> to_num(const std::string &string) {
  return std::pair<short int, T>(false, 0);
}
