#include "URI.h"
#include "logger.h"
#include <iostream>
#include <regex>

namespace uri {

void URI::UpdateHostPort() {
  HostPort = Port != 0 ? fmt::format("{}:{}", Host, Port) : Host;
}

URI::URI(const std::string &URIString) { parse(URIString); }

void URI::parse(const std::string &URIString) {
  std::smatch Matches;
  std::regex Regex(
      R"(\s*(([^:/?#]+):)?(//([^/?#:]+))(:(\d+))?/?([a-zA-Z0-9._-]+)?\s*)");
  std::regex_match(URIString, Matches, Regex);
  if (!Matches[4].matched) {
    throw std::runtime_error("Host not found when trying to parse URI");
  }
  Host = Matches.str(4);
  if (Matches[6].matched)
    Port = static_cast<uint32_t>(std::stoi(Matches.str(6)));
  if (Matches[7].matched)
    Topic = Matches.str(7);

  UpdateHostPort();
}
} // namespace uri
