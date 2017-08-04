#pragma once

#ifndef use_pcre2
#define use_pcre2 0
#endif
#include <array>
#include <string>
#if use_pcre2
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#else
#include <pcre.h>
#endif
#if HAVE_GTEST
#include <gtest/gtest.h>
#endif

namespace uri {

using std::array;
using std::string;

#if use_pcre2
using rchar = unsigned char;
using pcre_re_t = pcre2_code;
using pcre_erroff_t = PCRE2_SIZE;
using PCREX_SIZE = PCRE2_SIZE;
using PCREX_SPTR = PCRE2_SPTR;
#else
using rchar = char;
using pcre_re_t = pcre;
using pcre_erroff_t = int;
using PCREX_SIZE = int;
using PCREX_SPTR = char const *;
#endif

struct MD {
  MD(PCREX_SPTR subject);
  string substr(int i);
  PCREX_SPTR subject;
  bool ok = false;
  int ncap = 0;
#if use_pcre2
  ~MD();
  pcre2_match_data *md;
  array<PCREX_SIZE, 24> ovec;
#else
  array<int, 24> ovec;
#endif
};

struct Re {
#if use_pcre2
  Re(char const *s);
#endif
  Re(rchar const *s);
  ~Re();
  Re(Re &&);
  Re &operator=(Re &&);
  void init(rchar const *s);
  MD match(string const &s);
  pcre_re_t *re = nullptr;
  friend void swap(Re &x, Re &y);
};

struct static_ini {
  static_ini();
};

class URI {
public:
  using uchar = unsigned char;
  ~URI();
  URI();
  URI(std::string uri);
  void init(std::string uri);
  void default_host(std::string host);
  void default_port(int port);
  void default_path(std::string path);
  bool is_kafka_with_topic() const;
  std::string scheme;
  std::string host;
  std::string host_port;
  uint32_t port = 0;
  std::string path;
  std::string topic;
  bool require_host_slashes = true;

private:
  static Re re1;
  static Re re_host_no_slashes;
  static Re re_no_host;
  static Re re_topic;
  static static_ini compiled;
  void update_deps();
  friend struct static_ini;
};
}
