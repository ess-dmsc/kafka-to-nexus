// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Errors.h"

using StreamerStatus = FileWriter::Status::StreamerStatus;

std::string FileWriter::Status::StatusDescription(const StreamerStatus &Error) {
  switch (Error) {
  case StreamerStatus::OK:
    return "No error.";
  case StreamerStatus::WRITING:
    return "Writing";
  case StreamerStatus::HAS_FINISHED:
    return "Has finished";
  case StreamerStatus::INITIALISATION_FAILED:
    return "Initialisation failed";
  case StreamerStatus::NOT_INITIALISED:
    return "Not initialised";
  default:
    return "Unknown error code";
  }
}
