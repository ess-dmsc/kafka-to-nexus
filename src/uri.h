#pragma once

#include <array>
#include <regex>
#include <string>

namespace uri {

using std::array;
using std::vector;
using std::string;

/// Match data from a regex_match
struct MD {
  /// Keep the pointer to the haystack
  MD(char const *subject);
  /// Whether the match was successful or not
  bool ok = false;
  /// Returns the ith match or string() if out of bounds
  string substr(uint8_t i);
  /// Holds the matched substrings
  vector<string> matches;
  /// The haystack
  char const *subject;
};

/// Wraps a std::regex
struct Re {
  Re(std::regex const *re);
  Re(Re &&);
  Re &operator=(Re &&);
  MD match(string const &s) const;
  std::regex const *re = nullptr;
  friend void swap(Re &x, Re &y);
};

/// Thin parser for URIs.
class URI {
public:
  /// Creates a default URI with all members empty. You can (should) run
  /// URI::parse to fill it
  URI();
  /// Creates and parses the given URI
  URI(string uri);
  /// Parses the given `uri`
  void parse(string uri);
  /// Given a `http://www.example.com` scheme will contain `http`
  std::string scheme;
  /// Just the parsed hostname
  std::string host;
  /// If port was specified (or already non-zero before `URI::parse`) it
  /// contains `host:port`
  std::string host_port;
  /// The port number if specified, or zero to indicate that the port is not
  /// specified
  uint32_t port = 0;
  /// The path of the URL
  std::string path;
  /// If the path can be a valid Kafka topic name, then it is non-empty
  std::string topic;
  /// Whether we require two slashes before the hostname, as required by the
  /// standard.  Otherwise ambiguous because `host/path` could also be a
  /// `path/path`.
  bool require_host_slashes = true;

  /// Test the regular expression library at runtime.
  /// Some implementors ship a non-functional regex lib.
  static bool test();

private:
  static Re const re_full;
  static Re const re_host_no_slashes;
  static Re const re_no_host;
  static Re const re_topic;
  void update_deps();
};
}
