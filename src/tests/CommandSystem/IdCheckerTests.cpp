// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "CommandSystem/IdChecker.h"
#include <cstdlib>
#include <ctime>
#include <fmt/format.h>
#include <gtest/gtest.h>

namespace {
std::string const TestHostName{"some_host_name"};
int const TestPID{1235};
std::string const TestCmdStr{"SOME_CMD"};
} // namespace

std::string generateJobId() {
  auto Timestamp = time(nullptr);
  auto PartialId = fmt::format("{:s}-{:d}-{:08X}-{:04X}-", TestHostName,
                               TestPID, Timestamp ^ 0xFFFFFFFF, rand() % 65535);
  return PartialId + fmt::format("{:08X}", Command::adler32(PartialId));
}

std::string generateCmdId() {
  auto Timestamp = time(nullptr);
  auto PartialId =
      fmt::format("{:s}-{:d}-{:s}-{:08X}-{:04X}-", TestHostName, TestPID,
                  TestCmdStr, Timestamp ^ 0xFFFFFFFF, rand() % 65535);
  return PartialId + fmt::format("{:08X}", Command::adler32(PartialId));
}

TEST(JobId, FailWithEmptyString) {
  EXPECT_FALSE(Command::isJobIdValid("").first);
}

TEST(JobId, SuccessThenFailure) {
  auto UsedJobId = generateJobId();
  auto Result = Command::isJobIdValid(UsedJobId);
  EXPECT_TRUE(Result.first) << Result.second;
  EXPECT_FALSE(Command::isJobIdValid(UsedJobId).first);
}

TEST(JobId, OkTimestamp) {
  auto UsedJobId = generateJobId();
  auto Time = system_clock::now() + 5s;
  auto Result = Command::isJobIdValid(UsedJobId, Time);
  EXPECT_TRUE(Result.first) << Result.second;
}

TEST(JobId, BadTimestamp) {
  auto UsedJobId = generateJobId();
  auto Time = system_clock::now() + 15s;
  auto Result = Command::isJobIdValid(UsedJobId, Time);
  EXPECT_FALSE(Result.first) << Result.second;
}

TEST(JobId, BadCheckSum) {
  auto UsedJobId = generateJobId();
  UsedJobId[0] = 'S';
  auto Result = Command::isJobIdValid(UsedJobId);
  EXPECT_FALSE(Result.first) << Result.second;
}

TEST(CmdId, FailWithEmptyString) {
  EXPECT_FALSE(Command::isCmdIdValid("").first);
}

TEST(CmdId, SuccessThenFailure) {
  auto UsedCmdId = generateCmdId();
  auto Result = Command::isCmdIdValid(UsedCmdId);
  EXPECT_TRUE(Result.first) << Result.second;
  EXPECT_FALSE(Command::isCmdIdValid(UsedCmdId).first);
}

TEST(CmdId, OkTimestamp) {
  auto UsedCmdId = generateCmdId();
  auto Time = system_clock::now() + 5s;
  auto Result = Command::isCmdIdValid(UsedCmdId, Time);
  EXPECT_TRUE(Result.first) << Result.second;
}

TEST(CmdId, BadTimestamp) {
  auto UsedCmdId = generateCmdId();
  auto Time = system_clock::now() + 15s;
  auto Result = Command::isCmdIdValid(UsedCmdId, Time);
  EXPECT_FALSE(Result.first) << Result.second;
}

TEST(CmdId, BadCheckSum) {
  auto UsedCmdId = generateCmdId();
  UsedCmdId[0] = 'S';
  auto Result = Command::isCmdIdValid(UsedCmdId);
  EXPECT_FALSE(Result.first) << Result.second;
}

TEST(Adler32, EmptyStr) { EXPECT_EQ(Command::adler32(""), 1u); }

TEST(Adler32, TestStr1) { EXPECT_EQ(Command::adler32("AbCd12"), 104989102u); }

TEST(Adler32, TestStr2) {
  EXPECT_EQ(Command::adler32("AbCd12_?:asdfnsdkl_213214"), 1758070732u);
}
