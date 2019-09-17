// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "helper.h"
#include <fstream>
#include <unistd.h>

// getpid()
#include <sys/types.h>

int getpid_wrapper() { return getpid(); }

// Wrapper, because it may need some Windows implementation in the future.
std::string gethostname_wrapper() {
  std::vector<char> Buffer;
  Buffer.resize(1024);
  int Result = gethostname(Buffer.data(), Buffer.size());
  Buffer.back() = '\0';
  if (Result != 0) {
    return "";
  }
  return Buffer.data();
}

std::vector<char> readFileIntoVector(std::string const &FileName) {
  std::vector<char> ret;
  std::ifstream ifs(FileName, std::ios::binary | std::ios::ate);
  if (!ifs.good()) {
    return ret;
  }
  auto n1 = ifs.tellg();
  if (n1 <= 0) {
    return ret;
  }
  ret.resize(n1);
  ifs.seekg(0);
  ifs.read(ret.data(), n1);
  return ret;
}
