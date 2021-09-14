// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "helper.h"
#include <unistd.h>

#include <algorithm>
#include <random>

std::string randomHexString(size_t Length) {
  std::string const hexChars = "0123456789abcdef";
  std::mt19937_64 gen{std::random_device()()};

  std::uniform_int_distribution<size_t> dist{0, hexChars.size() - 1};

  std::string ReturnString;

  std::generate_n(std::back_inserter(ReturnString), Length,
                  [&] { return hexChars[dist(gen)]; });
  return ReturnString;
}

int getPID() { return getpid(); }

// Wrapper, because it may need some Windows implementation in the future.
std::string getHostName() {
  std::vector<char> Buffer;
  Buffer.resize(1024);
  int Result = gethostname(Buffer.data(), Buffer.size());
  Buffer.back() = '\0';
  if (Result != 0) {
    return "";
  }
  return Buffer.data();
}
