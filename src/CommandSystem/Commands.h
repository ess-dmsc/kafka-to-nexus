// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

#include "TimeUtility.h"
#include "URI.h"
#include <string>

namespace Command {

struct StartInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  uri::URI BrokerInfo{"localhost:9092"};
  std::chrono::milliseconds StartTime{0};
  time_point StopTime{time_point::max()};
};

struct StartMessage : public StartInfo {
  std::string ServiceID;
};

struct StopMessage {
  std::string JobID;
  std::chrono::milliseconds StopTime{0};
  std::string ServiceID;
};

} // namespace Command
