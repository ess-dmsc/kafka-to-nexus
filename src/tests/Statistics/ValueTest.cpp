// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Statistics/Value.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

using std::string_literals::operator""s;

TEST(Statistics, IntValue) {
  Statistics::Value<int> UnderTest{"/", "SomeKey"};
  int TestValue{1235};
  UnderTest.setValue(TestValue);

  EXPECT_EQ(TestValue, UnderTest.getValue());
  EXPECT_EQ(UnderTest.getAsJSON(), R"({"/:SomeKey":1235})"_json);
}

TEST(Statistics, IntVectorValue) {
  Statistics::Value<std::vector<int>> UnderTest{"/", "SomeKey"};
  std::vector<int> TestValue{1, 2, 3, 4};
  UnderTest.setValue(TestValue);

  EXPECT_EQ(TestValue, UnderTest.getValue());
  EXPECT_EQ(UnderTest.getAsJSON(), R"({"/:SomeKey":[1,2,3,4]})"_json);
}

TEST(Statistics, StringValue) {
  Statistics::Value<std::string> UnderTest{"/", "SomeKey"};
  std::string TestValue{"hello"};
  UnderTest.setValue(TestValue);

  EXPECT_EQ(TestValue, UnderTest.getValue());
  EXPECT_EQ(UnderTest.getAsJSON(), R"({"/:SomeKey":"hello"})"_json);
}
