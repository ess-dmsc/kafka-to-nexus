#include "helper.h"
#include <array>
#include <fstream>
#include <unistd.h>

// getpid()
#include <sys/types.h>
#include <unistd.h>

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

std::vector<char> binaryToHex(char const *data, uint32_t len) {
  std::vector<char> ret;
  ret.reserve(len * (64 + 5) / 32 + 32);
  for (uint32_t i1 = 0; i1 < len; ++i1) {
    uint8_t c = static_cast<uint8_t>(data[i1]) >> 4;
    if (c < 10) {
      c += 48;
    } else {
      c += 97 - 10;
    }
    ret.emplace_back(c);
    c = 0x0f & static_cast<uint8_t>(data[i1]);
    if (c < 10) {
      c += 48;
    } else {
      c += 97 - 10;
    }
    ret.emplace_back(c);
    if ((0x07 & i1) == 0x7) {
      ret.push_back(' ');
      if ((0x1f & i1) == 0x1f) {
        ret.push_back('\n');
      }
    }
  }
  return ret;
}

std::vector<std::string> split(std::string const &input,
                               std::string const &token) {
  using std::string;
  using std::vector;
  vector<string> ret;
  if (token.empty()) {
    return {input};
  }
  string::size_type i1 = 0;
  while (true) {
    auto i2 = input.find(token, i1);
    if (i2 == string::npos) {
      break;
    }
    if (i2 > i1) {
      ret.push_back(input.substr(i1, i2 - i1));
    }
    i1 = i2 + 1;
  }
  if (i1 != input.size()) {
    ret.push_back(input.substr(i1));
  }
  return ret;
}
