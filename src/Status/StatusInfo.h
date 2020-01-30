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
  std::string JobId{""};
  std::string Filename{""};
  std::chrono::milliseconds StartTime{0};
  std::chrono::milliseconds StopTime{0};
};

} // namespace Status
