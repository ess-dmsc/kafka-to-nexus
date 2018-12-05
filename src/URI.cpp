#include "URI.h"
#include "logger.h"
#include <iostream>
#include <regex>

namespace uri {

static std::string findTopicFromPath(std::string PathString) {
  //  auto Path = PathString.find_last_of('/');
  //  if (Path == std::string::npos) {
  //    return PathString;
  //  } else if (Path == 0) {
  //    return PathString.substr(1);
  //  } else {
  //    return PathString.substr(Path + 1, PathString.npos);
  //  }

  auto p = PathString.find('/');
  if (p == 0) {
    PathString = PathString.substr(1);
  }
  p = PathString.find('/');
  if (p == std::string::npos) {
    return PathString;
  } else {
    if (p == 0) {
      return PathString.substr(1);
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
  std::regex Regex(
      R"(\s*(([^:/?#]+):)?(//([^/?#:]+))(:(\d+))?([^?#]*)(\?([^#]*))?(#(.*))?\s*)");
  std::regex_match(URIString, Matches, Regex);
  if (!Matches[4].matched) {
    throw std::runtime_error("Host not found when trying to parse URI");
  }
  Host = Matches.str(4);
  if (Matches[6].matched)
    Port = static_cast<uint32_t>(std::stoi(Matches.str(6)));
  if (Matches[7].matched)
    Path = Matches.str(7);

  updateHostPortAndTopic();
}
} // namespace uri
