// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "EventLogger.h"

std::string FileWriter::convertStatusCodeToString(FileWriter::StatusCode Code) {
  switch (Code) {
  case FileWriter::StatusCode::Start:
    return "START";
  case FileWriter::StatusCode::Close:
    return "CLOSE";
  case FileWriter::StatusCode::Error:
    return "ERROR";
  case FileWriter::StatusCode::Fail:
    return "FAIL";
  default:
    return "UNKNOWN";
  }
};
