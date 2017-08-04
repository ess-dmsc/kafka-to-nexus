#include "uri.h"
#include "logger.h"
#include <array>

namespace uri {

using std::array;
using std::move;
using std::swap;

struct CG {
  PCREX_SIZE a, b, n;
  char const *s;
  std::string substr(char const *p0) { return std::string(p0 + a, n); }
  std::string substr() { return std::string(s + a, n); }
};

#if use_pcre2
MD::MD(PCREX_SPTR subject) : subject(subject) {
  md = pcre2_match_data_create(16, nullptr);
  if (!md) {
    throw std::runtime_error("allocation failed");
  }
}
MD::~MD() { pcre2_match_data_free(md); }
string MD::substr(int i) {
  if (!ok)
    string();
  if (i >= ncap)
    string();
  auto ov = pcre2_get_ovector_pointer(md) + 2 * i;
  if (ov[0] == PCRE2_UNSET)
    return string();
  string::size_type n(ov[1] - ov[0]);
  if (n > 0)
    return {(char *)subject + ov[0], n};
  return string();
}
#else
MD::MD(PCREX_SPTR subject) : subject(subject) {}
string MD::substr(int i) {
  if (!ok)
    string();
  if (i >= ncap)
    string();
  auto ov = ovec.data() + 2 * i;
  if (ov[0] < 0)
    return string();
  string::size_type n(ov[1] - ov[0]);
  if (n > 0)
    return {subject + ov[0], n};
  return string();
}
#endif

#if use_pcre2
void p_regerr(int err) {
  std::array<unsigned char, 256> s1;
  auto n = pcre2_get_error_message(err, s1.data(), s1.size());
  fmt::print("err in regex: [{}, {}] {:.{}}\n", err, n, (char *)s1.data(), n);
}
#else
void p_regerr(char const *s1) { fmt::print("err in regex: {}\n", s1); }
#endif

#if use_pcre2
Re::Re(char const *s) { init((rchar const *)s); }
Re::Re(rchar const *s) { init(s); }
void Re::init(rchar const *s) {
  int err = 0;
  pcre_erroff_t errpos = 0;
  re = pcre2_compile_8(s, PCRE2_ZERO_TERMINATED, 0, &err, &errpos, nullptr);
  if (!re) {
    p_regerr(err);
    throw std::runtime_error("can not compile regex");
  }
}
Re::~Re() { pcre2_code_free(re); }
MD Re::match(string const &s) {
  MD md((PCREX_SPTR)s.data());
  auto x =
      pcre2_match(re, (PCREX_SPTR)s.data(), s.size(), 0, 0, md.md, nullptr);
  if (x >= 0) {
    md.ok = true;
  }
  return md;
}
#else
Re::Re(rchar const *s) {
  if (!s) {
    throw std::runtime_error("empty regular expression");
  }
  char const *errptr = nullptr;
  pcre_erroff_t errpos = 0;
  re = pcre_compile(s, 0, &errptr, &errpos, nullptr);
  if (!re) {
    p_regerr(errptr);
    throw std::runtime_error("can not compile regex");
  }
}
Re::~Re() { pcre_free(re); }
MD Re::match(string const &s) {
  MD md(s.data());
  auto x = pcre_exec(re, nullptr, s.data(), s.size(), 0, 0, md.ovec.data(),
                     md.ovec.size());
  if (x > 0) {
    md.ok = true;
    md.ncap = x;
  }
  return md;
}
#endif
Re::Re(Re &&x) { swap(*this, x); }
Re &Re::operator=(Re &&x) {
  swap(*this, x);
  return *this;
}
void swap(Re &x, Re &y) { swap(x.re, y.re); }

static_ini::static_ini() {
  URI::re1 = Re((rchar *)"^\\s*(([a-z]+):)?//(([-._A-Za-z0-9]+)(:([0-9]+))?)(/"
                         "[-./_A-Za-z0-9]*)?\\s*$");
  URI::re_host_no_slashes = Re(
      (rchar *)"^\\s*(([-._A-Za-z0-9]+)(:([0-9]+))?)(/[-./_A-Za-z0-9]*)?\\s*$");
  URI::re_no_host = Re((rchar *)"^/?([-./_A-Za-z0-9]*)$");
  URI::re_topic = Re((rchar *)"^/?([-._A-Za-z0-9]+)$");
}

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

URI::~URI() {}

URI::URI() {}

URI::URI(std::string uri) { init(uri); }

void URI::init(std::string uri) {
  using std::vector;
  using std::string;
  bool match = false;
  if (!match) {
    auto md = re1.match(uri);
    if (md.ok) {
      match = true;
      scheme = md.substr(2);
      host = md.substr(4);
      auto port_s = md.substr(6);
      if (port_s.size() > 0) {
        port = strtoul(port_s.data(), nullptr, 10);
      }
      path = md.substr(7);
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
      path = md.substr(5);
    }
  }
  if (!match) {
    auto md = re_no_host.match(uri);
    if (md.ok) {
      match = true;
      path = md.substr(0);
    }
  }
  update_deps();
}

Re URI::re1(".");
Re URI::re_host_no_slashes(".");
Re URI::re_no_host(".");
Re URI::re_topic(".");

void URI::default_port(int port_) {
  if (port == 0) {
    port = port_;
  }
  update_deps();
}

void URI::default_path(std::string path_) {
  if (path.size() == 0) {
    path = path_;
  }
  update_deps();
}

void URI::default_host(std::string host_) {
  if (host.size() == 0) {
    host = host_;
  }
  update_deps();
}

static_ini URI::compiled;
}
