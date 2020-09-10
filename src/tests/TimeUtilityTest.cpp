// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "TimeUtility.h"
#include <gtest/gtest.h>
#include <regex>

TEST(TimeUtility, TimePointToUTCString) {
  time_point TestTime(1ms);
  std::string const ExpectedResult{"1970-01-01T00:00:00.001Z"};
  EXPECT_EQ(ExpectedResult, toUTCDateTime(TestTime));
}

TEST(TimeUtility, TimePointToLocalString) {
  time_point TestTime(2ms);
  std::regex TestRegex(
      "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.002\\+\\d{4}");
  auto TimeString = toLocalDateTime(TestTime);
  EXPECT_TRUE(std::regex_match(TimeString, TestRegex));
}
