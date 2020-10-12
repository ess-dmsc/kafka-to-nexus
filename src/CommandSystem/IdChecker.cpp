// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "IdChecker.h"
#include <regex>

namespace Command {

bool isJobIdValid(std::string const &JobId) {
  const std::regex JobIdRegex{
      "((.*)-(\\d+)-([A-F0-9]{8})-([A-F0-9]{4})-)([A-F0-9]{8})"};
  std::smatch Match;
  std::regex_match(JobId, Match, JobIdRegex);
  if (Match.empty()) {
    return false;
  }
  return true;
}
bool isCmdIdValid(std::string const &CmdId) {
  const std::regex CmdIdRegex{
      "((.*)-(\\\\d+)-(.*)-([A-F0-9]{8})-([A-F0-9]{4})-)([A-F0-9]{8})"};
  std::smatch Match;
  std::regex_match(CmdId, Match, CmdIdRegex);
  return not CmdId.empty();
}

} // namespace Command
