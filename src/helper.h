#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

uint64_t getpid_wrapper();

std::string gethostname_wrapper();

std::vector<char> gulp(std::string fname);

std::vector<char> binary_to_hex(char const *data, uint32_t len);

std::vector<std::string> split(std::string const &input, std::string token);
