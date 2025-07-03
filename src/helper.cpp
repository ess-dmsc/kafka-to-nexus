// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "helper.h"
#include "logger.h"
#include <algorithm>
#include <netdb.h>
#include <random>
#include <unistd.h>

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

std::string getHostNameWithUnderscore() {
  std::string Host = getHostName();
  std::replace(Host.begin(), Host.end(), '.', '_');
  return Host;
}

std::string getFQDN() {
  auto const HostName = getHostName();

  addrinfo hints, *info, *p;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_CANONNAME;

  int GetAddressResult{0};
  if ((GetAddressResult =
           getaddrinfo(HostName.c_str(), nullptr, &hints, &info)) != 0) {
    Logger::Info(
        R"(Unable to get FQDN due to error ("{}"), using hostname instead.)",
        gai_strerror(GetAddressResult));
    return HostName;
  }

  std::vector<std::string> FoundHostnames;
  for (p = info; p != nullptr; p = p->ai_next) {
    if (p->ai_canonname != nullptr) {
      FoundHostnames.push_back(std::string(p->ai_canonname));
    }
  }
  freeaddrinfo(info);

  auto Longest = std::max_element(
      FoundHostnames.cbegin(), FoundHostnames.cend(),
      [](auto const &lhs, auto const &rhs) { return lhs.size() < rhs.size(); });
  if (Longest == FoundHostnames.end()) {
    Logger::Info("No FQDN found, using hostname instead.");
    return HostName;
  }
  return *Longest;
}
