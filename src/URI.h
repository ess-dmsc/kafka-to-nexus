#pragma once

#include <array>
#include <string>
#include <vector>

namespace uri {

using std::array;
using std::vector;

/// \brief Thin parser for URIs.
class URI {
public:
  /// \brief Creates a default URI with all members empty. You can (should) run
  /// URI::parse to fill it
  URI() = default;

  /// Creates and parses the given URI
  explicit URI(std::string URIString);

  /// Parses the given `uri`
  void parse(std::string URIString);

  /// Given a `http://www.example.com` scheme will contain `http`.
  std::string Scheme;

  /// Parsed hostname.
  std::string Host;

  /// \brief If port was specified (or already non-zero before `URI::parse`) it
  /// contains `host:port`.
  std::string HostPort;

  /// \brief The port number if specified, or zero to indicate that the port is
  /// not specified.
  uint32_t Port = 0;

  /// The path of the URL.
  std::string Path;

  /// If the path can be a valid Kafka topic name, then it is non-empty.
  std::string Topic;

  /// \brief Whether we require two slashes before the hostname, as required by
  /// the standard.
  ///
  /// Otherwise ambiguous because `host/path` could also be a `path/path`.
  bool RequireHostSlashes = true;

  /// The URI string //<host>:<port>/<topic>
  std::string getURIString() { return "//" + HostPort + "/" + Topic; }

private:
  void updateHostPortAndTopic();
};
} // namespace uri
