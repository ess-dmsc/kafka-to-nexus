#pragma once

#include <array>
#include <string>
#include <vector>

namespace uri {

using std::array;
using std::vector;
using std::string;

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

private:
  void update_deps();
};
}
