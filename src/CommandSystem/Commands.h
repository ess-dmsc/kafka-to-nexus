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

/// \brief Structure for holding all the data required to start a file-writing job.
struct StartInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string Metadata;
  uri::URI BrokerInfo{"localhost:9092"};
  std::chrono::milliseconds StartTime{0};
  time_point StopTime{time_point::max()};
};

/// \brief A de-serialised "start writing" message.
struct StartMessage : StartInfo {
  std::string ServiceID;
};

/// \brief A de-serialised "set stop time" message.
struct StopMessage {
  std::string JobID;
  std::string CommandID;
  std::chrono::milliseconds StopTime{0};
  std::string ServiceID;
};

} // namespace Command
