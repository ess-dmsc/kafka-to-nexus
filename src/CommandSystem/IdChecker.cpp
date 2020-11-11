// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "IdChecker.h"
#include <fmt/format.h>
#include <regex>

namespace Command {
static const int CommandTimeLimit{10};

constexpr uint32_t MOD_ADLER = 65521;

uint32_t adler32(std::string const &Input) {
  uint32_t a = 1, b = 0;
  for (size_t i = 0; i < Input.size(); ++i) {
    a = (a + Input[i]) % MOD_ADLER;
    b = (b + a) % MOD_ADLER;
  }
  return (b << 16) | a;
}

std::string const JobIdNoMatchString{
    "Job-Id did not match the reg.-ex. (id was: \"{:s}\""};
std::string const JobIdCheckSumWrong{
    "Job-Id checksum was wrong. It was: 0x{:s}, it should be 0x{:08X}"};
std::string const JobIdTimestampBad{"Job-Id creation time is bad."};
std::string const JobIdIdentifierKnown{
    "Job-Id (\"{}\")has already been used and is therefore rejected."};

std::pair<bool, std::string> isJobIdValid(std::string const &JobId,
                                          time_point Now) {
  const std::regex JobIdRegex{
      "((.+)-(\\d+)-([A-F0-9]{8})-([A-F0-9]{4})-)([A-F0-9]{8})"};
  std::smatch Match;
  std::regex_match(JobId, Match, JobIdRegex);
  if (Match.empty()) {
    return {false, fmt::format(JobIdNoMatchString, JobId)};
  }
  int64_t Timestamp = std::stoll(Match[4], nullptr, 16) ^ 0xFFFFFFFF;
  if (abs(toSeconds(Now) - Timestamp) > CommandTimeLimit) {
    return {false, JobIdTimestampBad};
  }
  auto CalculatedCheckSum = adler32(Match[1]);
  if (Match[6] != fmt::format("{:08X}", CalculatedCheckSum)) {
    return {false, fmt::format(JobIdCheckSumWrong, std::string(Match[6]),
                               CalculatedCheckSum)};
  }
  static IdTracker KnownIDs;
  if (not KnownIDs.checkAndRegisterNewId(JobId)) {
    return {false, fmt::format(JobIdIdentifierKnown, JobId)};
  }
  return {true, {}};
}

std::string const CmdIdNoMatchString{"Command-Id did not match the reg.-ex."};
std::string const CmdIdCheckSumWrong{"Command-Id checksum was wrong."};
std::string const CmdIdTimestampBad{"Command-Id creation time is bad."};
std::string const CmdIdIdentifierKnown{
    "Command-Id has already been used and is therefore rejected."};

std::pair<bool, std::string> isCmdIdValid(std::string const &CmdId,
                                          time_point Now) {
  const std::regex CmdIdRegex{
      "((.+)-(\\d+)-(.+)-([A-F0-9]{8})-([A-F0-9]{4})-)([A-F0-9]{8})"};
  std::smatch Match;
  std::regex_match(CmdId, Match, CmdIdRegex);
  if (Match.empty()) {
    return {false, CmdIdNoMatchString};
  }
  if (Match[7] != fmt::format("{:08X}", adler32(Match[1]))) {
    return {false, CmdIdCheckSumWrong};
  }
  int64_t Timestamp = std::stoll(Match[5], nullptr, 16) ^ 0xFFFFFFFF;
  if (abs(toSeconds(Now) - Timestamp) > CommandTimeLimit) {
    return {false, CmdIdTimestampBad};
  }
  static IdTracker KnownIDs;
  if (not KnownIDs.checkAndRegisterNewId(CmdId)) {
    return {false, CmdIdIdentifierKnown};
  }
  return {true, {}};
}

bool IdTracker::checkAndRegisterNewId(const std::string &NewId) {
  std::lock_guard Guard(SetMutex);
  return PastIDs.emplace(std::hash<std::string>{}(NewId)).second;
}

} // namespace Command
