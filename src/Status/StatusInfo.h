// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include <chrono>
#include <string>

namespace Status {

struct StatusInfo {
  std::string jobId{""};
  std::string filename{""};
  std::chrono::milliseconds startTime{0};
};

} // namespace Status
