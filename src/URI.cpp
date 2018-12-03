#include "URI.h"
#include "logger.h"
#include <iostream>

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
  if (Port != 0) {
    HostPort = fmt::format("{}:{}", Host, Port);
  } else {
    HostPort = Host;
  }
  auto TopicFromPath = findTopicFromPath(Path);
  if (not TopicFromPath.empty()) {
    Topic = TopicFromPath;
  }
}

URI::URI(std::string URIString) { parse(URIString); }

static vector<std::string> findProtocol(const std::string &URIString) {
  auto Slashes = URIString.find("://");
  if (Slashes == std::string::npos or Slashes == 0) {
    return {std::string(), URIString};
  }
  auto Protocol = URIString.substr(0, Slashes);
  if (Protocol.find_first_of("0123456789") != std::string::npos) {
    return {std::string(), URIString};
  }
  return {Protocol, URIString.substr(Slashes + 1, std::string::npos)};
}

static vector<std::string> findHostAndPort(const std::string &URIString) {
  if (URIString.find("//") == std::string::npos) {
    return {std::string(), std::string(), URIString};
  }
  auto SlashPos = URIString.find("/", 2);
  auto ColonPos = URIString.find(":", 2);
  if (ColonPos == std::string::npos) {
    if (SlashPos == std::string::npos) {
      return {URIString.substr(2), std::string(), std::string()};
    } else {
      return {URIString.substr(2, SlashPos - 2), std::string(),
              URIString.substr(SlashPos)};
    }
  } else {
    if (SlashPos == std::string::npos) {
      return {URIString.substr(2, ColonPos - 2), URIString.substr(ColonPos + 1),
              std::string()};
    } else {
      if (ColonPos < SlashPos) {
        return {URIString.substr(2, ColonPos - 2),
                URIString.substr(ColonPos + 1, SlashPos - ColonPos - 1),
                URIString.substr(SlashPos)};
      } else {
        return {URIString.substr(2, SlashPos - 2), std::string(),
                URIString.substr(SlashPos)};
      }
    }
  }
}

static std::string trim(std::string URIString) {
  std::string::size_type a = 0;
  while (URIString.find(' ', a) == a) {
    ++a;
  }
  URIString = URIString.substr(a);
  if (URIString.empty()) {
    return URIString;
  }
  a = URIString.size() - 1;
  while (URIString[a] == ' ') {
    --a;
  }
  URIString = URIString.substr(0, a + 1);
  return URIString;
}

void URI::parse(std::string URIString) {
  URIString = trim(URIString);
  auto Protocol = findProtocol(URIString);
  if (not Protocol[0].empty()) {
    Scheme = Protocol[0];
  }
  auto s = Protocol[1];
  if (not RequireHostSlashes) {
    if (s.find('/') != 0) {
      s = "//" + s;
    }
  }
  auto HostPort = findHostAndPort(s);
  if (not HostPort[0].empty()) {
    Host = HostPort[0];
  }
  if (not HostPort[1].empty()) {
    Port = static_cast<uint32_t>(strtoul(HostPort[1].data(), nullptr, 10));
  }
  if (not HostPort[2].empty()) {
    Path = HostPort[2];
  }
  updateHostPortAndTopic();
}
} // namespace uri
