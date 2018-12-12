#include "URI.h"
#include "logger.h"
#include <iostream>
#include <regex>

namespace uri {

URI::URI(const std::string &URIString) { parse(URIString); }

void URI::parse(const std::string &URIString) {
  std::smatch Matches;
  // This is a modified version of the RFC-3986 Regex for capturing URIs. The
  // host is mandatory however scheme and port are optional. Uses capture groups
  // for each component of the URI and ignores any paths
  std::regex Regex(
      R"(\s*(([^:/?#]+):)?//((([^/?#:]+)+)(:(\d+))?)/?([a-zA-Z0-9._-]+)?\s*)");
  std::regex_match(URIString, Matches, Regex);
  if (!Matches[4].matched) {
    throw std::runtime_error("Host not found when trying to parse URI");
  }
  HostPort = Matches.str(3);
  if (Matches[7].matched)
    Port = static_cast<uint32_t>(std::stoi(Matches.str(7)));
  if (Matches[8].matched)
    Topic = Matches.str(8);
}
} // namespace uri
