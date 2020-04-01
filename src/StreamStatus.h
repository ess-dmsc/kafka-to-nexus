// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file This file defines the different success and failure status that the
/// `StreamController` and the `Streamer` can incur. These error object have some
/// utility methods that can be used to test the more common situations.

#pragma once
#include <string>

namespace FileWriter {

/// \brief Class that lists possible states and helps the Streamer to define its
/// status.
enum class StreamerStatus {
  OK = 1000,
  WRITING = 2,
  HAS_FINISHED = 1,
  IS_CONNECTED = 0,
  NOT_INITIALISED = -1000,
  INITIALISATION_FAILED = -1
};

/// \brief Converts a StreamerError status into a human readable string.
///
/// \param Error The error status.
///
/// \return A string that briefly describes the status.
std::string StatusDescription(const StreamerStatus &Error);

} // namespace FileWriter
