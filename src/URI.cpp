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
      R"(\s*((([^:/?#]+)://)|(//)|())((([^/?#:]+)+)(:(\d+))?)/?([a-zA-Z0-9._-]+)?\s*)");
  std::regex_match(URIString, Matches, Regex);
  if (!Matches[6].matched) {
    throw std::runtime_error("Unable to extract host from the URI: \"" +
                             URIString + "\".");
  }
  HostPort = Matches[6].str();
  if (Matches[10].matched) {
    Port = static_cast<uint32_t>(std::stoi(Matches[10].str()));
  }
  if (Matches[11].matched) {
    Topic = Matches[11].str();
  }
}
} // namespace uri
