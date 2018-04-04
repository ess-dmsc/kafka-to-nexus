#include "uri.h"
#include "logger.h"
#include <iostream>

namespace uri {

using std::array;
using std::move;

static string topic_from_path(string s) {
  auto p = s.find("/");
  if (p == 0) {
    s = s.substr(1);
  }
  p = s.find("/");
  if (p == string::npos) {
    return s;
  } else {
    if (p == 0) {
      return s.substr(1);
    } else {
      return string();
    }
  }
}

void URI::update_deps() {
  if (port != 0) {
    host_port = fmt::format("{}:{}", host, port);
  } else {
    host_port = host;
  }
  auto t = topic_from_path(path);
  if (not t.empty()) {
    topic = t;
  }
}

URI::URI() {}

URI::URI(string uri) { parse(uri); }

static bool is_alpha(string s) {
  for (auto c : s) {
    if (c < 'a' or c > 'z') {
      return false;
    }
  }
  return true;
}

static vector<string> protocol(string s) {
  auto slashes = s.find("://");
  if (slashes == string::npos or slashes == 0) {
    return {string(), s};
  }
  auto proto = s.substr(0, slashes);
  if (not is_alpha(proto)) {
    return {string(), s};
  }
  return {proto, s.substr(slashes + 1, string::npos)};
}

static vector<string> hostport(string s) {
  if (s.find("//") != 0) {
    return {string(), string(), s};
  }
  auto slash = s.find("/", 2);
  auto colon = s.find(":", 2);
  if (colon == string::npos) {
    if (slash == string::npos) {
      return {s.substr(2), string(), string()};
    } else {
      return {s.substr(2, slash - 2), string(), s.substr(slash)};
    }
  } else {
    if (slash == string::npos) {
      return {s.substr(2, colon - 2), s.substr(colon + 1), string()};
    } else {
      if (colon < slash) {
        return {s.substr(2, colon - 2), s.substr(colon + 1, slash - colon - 1),
                s.substr(slash)};
      } else {
        return {s.substr(2, slash - 2), string(), s.substr(slash)};
      }
    }
  }
  return {string(), string(), s};
}

static string trim(string s) {
  string::size_type a = 0;
  while (s.find(' ', a) == a) {
    ++a;
  }
  s = s.substr(a);
  if (s.empty()) {
    return s;
  }
  a = s.size() - 1;
  while (s[a] == ' ') {
    --a;
  }
  s = s.substr(0, a + 1);
  return s;
}

void URI::parse(string uri) {
  uri = trim(uri);
  auto proto = protocol(uri);
  if (not proto[0].empty()) {
    scheme = proto[0];
  }
  auto s = proto[1];
  if (not require_host_slashes) {
    if (s.find('/') != 0) {
      s = "//" + s;
    }
  }
  auto hp = hostport(s);
  if (not hp[0].empty()) {
    host = hp[0];
  }
  if (not hp[1].empty()) {
    port = static_cast<uint32_t>(strtoul(hp[1].data(), nullptr, 10));
  }
  if (not hp[2].empty()) {
    path = hp[2];
  }
  update_deps();
}
} // namespace uri
