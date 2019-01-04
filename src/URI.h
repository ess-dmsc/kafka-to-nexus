#pragma once

#include <array>
#include <string>
#include <vector>

namespace uri {

/// \brief Thin parser for URIs.
struct URI {
  /// Creates and parses the given URI
  explicit URI(const std::string &URIString);

  /// Parses the given `uri`
  void parse(const std::string &URIString);

  /// \brief If port was specified (or already non-zero before `URI::parse`) it
  /// contains `host:port`.
  std::string HostPort;

  /// \brief The port number if specified, or zero to indicate that the port is
  /// not specified.
  uint32_t Port = 0;

  /// If the path can be a valid Kafka topic name, then it is non-empty.
  std::string Topic;

  /// The URI string //<host>:<port>/<topic>
  std::string getURIString() const { return "//" + HostPort + "/" + Topic; }
};
} // namespace uri
