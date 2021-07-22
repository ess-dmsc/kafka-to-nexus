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
#include <chrono>
#include <string>

namespace Status {

// This info changes each write job
struct JobStatusInfo {
  enum class WorkerState { Idle, Writing };
  WorkerState State{WorkerState::Idle};
  std::string JobId{""};
  std::string Filename{""};
  time_point StartTime{0ms};
  time_point StopTime{0ms};
};

// This info is constant for this instance of the software
struct ApplicationStatusInfo {
  // Time interval between publishing status messages
  duration const UpdateInterval;
  std::string const ApplicationName;
  std::string const ApplicationVersion;
  std::string const HostName;
  std::string const ServiceName;
  std::string const ServiceID;
  int32_t const ProcessID;
};

} // namespace Status
