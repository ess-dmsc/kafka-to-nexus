// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

/// \file This file defines the different success and failure status that the
/// `StreamMaster` and the `Streamer` can incur. These error object have some
/// utility methods that can be used to test the more common situations.

#pragma once
#include <string>

namespace FileWriter {
namespace Status {

/// Class that helps the `StreamMaster` to define its status.
enum class StreamMasterError {
  OK = 1000,
  NOT_STARTED = 0,
  RUNNING = 1,
  HAS_FINISHED = 2,
  EMPTY_STREAMER = 3,
  IS_REMOVABLE = 4,
  STREAMER_ERROR = -1,
  REPORT_ERROR = -2
};

/// \brief Class that lists possible states and helps the Streamer to define its
/// status.
enum class StreamerStatus {
  OK = 1000,
  WRITING = 2,
  HAS_FINISHED = 1,
  IS_CONNECTED = 0,
  NOT_INITIALIZED = -1000,
  CONFIGURATION_ERROR = -1,
  TOPIC_PARTITION_ERROR = -2,
  UNKNOWN_ERROR = -1001
};

/// \brief Converts a StreamerError status into a human readable string.
///
/// \param Error The error status.
///
/// \return A string that briefly describes the status.
const std::string Err2Str(const StreamerStatus &Error);

/// \brief Converts a StreamMasterError status into a human readable string.
///
/// \param Error The error status.
///
/// \return  A string that briefly describes the status.
const std::string Err2Str(const StreamMasterError &Error);

} // namespace Status
} // namespace FileWriter
