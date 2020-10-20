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
#include <mutex>
#include <set>
#include <string>

namespace Command {

uint32_t adler32(std::string const &Input);

class IdTracker {
public:
  IdTracker() = default;
  bool checkAndRegisterNewId(std::string const &NewId);

private:
  std::mutex SetMutex;
  std::set<size_t> PastIDs;
};

std::pair<bool, std::string> isJobIdValid(std::string const &JobId,
                                          time_point Now = system_clock::now());

std::pair<bool, std::string> isCmdIdValid(std::string const &CmdId,
                                          time_point Now = system_clock::now());

} // namespace Command
