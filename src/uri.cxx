#include "uri.h"
#include "logger.h"
#include <array>

namespace uri {

using std::array;
using std::move;
using std::swap;

struct Patterns {
  std::regex empty{".*"};
  std::regex full{"^\\s*(([a-z]+):)?//(([-._A-Za-z0-9]+)(:([0-9]+))?)(/[-./"
                  "_A-Za-z0-9]*)?\\s*$"};
  std::regex re_host_no_slashes{
      "^\\s*(([-._A-Za-z0-9]+)(:([0-9]+))?)(/[-./_A-Za-z0-9]*)?\\s*$"};
  std::regex re_no_host{"^/?([-./_A-Za-z0-9]*)$"};
  std::regex re_topic{"^/?([-._A-Za-z0-9]+)$"};
};

static Patterns patterns;

MD::MD(char const *subject) : subject(subject) {}

string MD::substr(uint8_t i) {
  if (!ok) {
    string();
  }
  if (i >= matches.size()) {
    return string();
  }
  return matches[i];
}

Re::Re(std::regex *re) : re(re) {}

MD Re::match(string const &s) {
  MD md(s.data());
  std::smatch m;
  auto str = string(s.data(), s.size());
  auto matched = std::regex_match(str, m, *re);
  if (matched) {
    md.ok = true;
    for (auto &it : m) {
      md.matches.push_back(it.str());
    }
  }
  return md;
}

Re::Re(Re &&x) { swap(*this, x); }

Re &Re::operator=(Re &&x) {
  swap(*this, x);
  return *this;
}

void swap(Re &x, Re &y) { swap(x.re, y.re); }

void URI::update_deps() {
  if (port != 0) {
    host_port = fmt::format("{}:{}", host, port);
  } else {
    host_port = host;
  }
  // check if the path could be a valid topic
  auto md = re_topic.match(path);
  if (md.ok) {
    topic = md.substr(1);
  }
}

URI::URI() {}

URI::URI(string uri) { parse(uri); }

void URI::parse(string uri) {
  bool match = false;
  if (!match) {
    auto md = re_full.match(uri);
    if (md.ok) {
      match = true;
      scheme = md.substr(2);
      host = md.substr(4);
      auto port_s = md.substr(6);
      if (port_s.size() > 0) {
        port = strtoul(port_s.data(), nullptr, 10);
      }
      auto path_str = md.substr(7);
      if (!path_str.empty()) {
        path = path_str;
      }
    }
  }
  if (!match && !require_host_slashes) {
    auto md = re_host_no_slashes.match(uri);
    if (md.ok) {
      match = true;
      host = md.substr(2);
      auto port_s = md.substr(4);
      if (port_s.size() > 0) {
        port = strtoul(port_s.data(), nullptr, 10);
      }
      auto path_str = md.substr(5);
      if (!path_str.empty()) {
        path = path_str;
      }
    }
  }
  if (!match) {
    auto md = re_no_host.match(uri);
    if (md.ok) {
      match = true;
      auto path_str = md.substr(0);
      if (!path_str.empty()) {
        path = path_str;
      }
    }
  }
  update_deps();
}

Re URI::re_full(&patterns.full);
Re URI::re_host_no_slashes(&patterns.re_host_no_slashes);
Re URI::re_no_host(&patterns.re_no_host);
Re URI::re_topic(&patterns.re_topic);
}
