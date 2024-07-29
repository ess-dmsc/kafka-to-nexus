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

/// \brief Structure for holding all the data required to start a file-writing
/// job.
struct StartInfo {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string Metadata;
  time_point StartTime{0ms};
  time_point StopTime{time_point::max()};
  std::string ControlTopic;
};

/// \brief A de-serialised "start writing" message.
struct StartMessage {
  std::string JobID;
  std::string Filename;
  std::string NexusStructure;
  std::string Metadata;
  time_point StartTime{0ms};
  time_point StopTime{time_point::max()};
  std::string ControlTopic;
  std::string ServiceID;
};

/// \brief A de-serialised "set stop time" message.
struct StopMessage {
  std::string JobID;
  std::string CommandID;
  time_point StopTime{0ms};
  std::string ServiceID;
};

} // namespace Command
