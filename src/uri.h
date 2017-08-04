#pragma once

#include <array>
#include <string>
#include <regex>

namespace uri {

using std::array;
using std::vector;
using std::string;

struct MD {
  MD(char const * subject);
  string substr(uint8_t i);
  char const * subject;
  bool ok = false;
  vector<string> matches;
};

struct Re {
  Re(std::regex * re);
  Re(Re &&);
  Re &operator=(Re &&);
  MD match(string const &s);
  std::string regex_str;
  std::regex *re = nullptr;
  friend void swap(Re &x, Re &y);
};

class URI {
public:
  using uchar = unsigned char;
  ~URI();
  URI();
  URI(string uri);
  void parse(string uri);
  bool is_kafka_with_topic() const;
  std::string scheme;
  std::string host;
  std::string host_port;
  uint32_t port = 0;
  std::string path;
  std::string topic;
  bool require_host_slashes = true;

private:
  static Re re_full;
  static Re re_host_no_slashes;
  static Re re_no_host;
  static Re re_topic;
  void update_deps();
};
}
