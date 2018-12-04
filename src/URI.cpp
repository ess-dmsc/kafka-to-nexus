#include "URI.h"
#include "logger.h"
#include <iostream>
#include <regex>

namespace uri {

using std::array;
using std::move;

static std::string findTopicFromPath(std::string URIString) {
  auto Path = URIString.find("/");
  if (Path == 0) {
    URIString = URIString.substr(1);
  }
  Path = URIString.find("/");
  if (Path == std::string::npos) {
    return URIString;
  } else {
    if (Path == 0) {
      return URIString.substr(1);
    } else {
      return std::string();
    }
  }
}

void URI::updateHostPortAndTopic() {
  HostPort = Port != 0 ? fmt::format("{}:{}", Host, Port) : Host;
  Topic = Topic.empty() ? findTopicFromPath(Path) : Topic;
}

URI::URI(std::string URIString) { parse(URIString); }

void URI::parse(std::string URIString) {
  std::smatch Matches;
  std::regex Regex(R"(\/\/([\w.]+):?(\d+)?([/\w-]+)?)");
  std::regex_search(URIString, Matches, Regex);

  Host = Matches.str(1);
  if (Matches[2].matched)
    Port = static_cast<uint32_t>(std::stoi(Matches.str(2)));
  if (Matches[3].matched)
    Path = Matches.str(3);

  updateHostPortAndTopic();
}
} // namespace uri
