// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Statistics/Tracker.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

TEST(StatisticsTracker, SingleValue) {
  Statistics::Tracker UnderTest;
  std::string Key1{"some_key1"};
  Statistics::Value<int> TestValue{"/", Key1};
  TestValue.setValue(12345);
  UnderTest.registerStatistic(TestValue);
  nlohmann::json TestJSON("{}"_json);
  UnderTest.writeToJSONDict(TestJSON);
  EXPECT_EQ(TestJSON, R"({"/:some_key1":12345})"_json);
}

TEST(StatisticsTracker, TwoValues) {
  Statistics::Tracker UnderTest;
  std::string Key1{"some_key1"};
  std::string Key2{"some_key2"};
  Statistics::Value<int> TestValue1{"/", Key1};
  TestValue1.setValue(12345);
  Statistics::Value<std::string> TestValue2{"/", Key2};
  TestValue2.setValue("hello");
  UnderTest.registerStatistic(TestValue1);
  UnderTest.registerStatistic(TestValue2);
  nlohmann::json TestJSON("{}"_json);
  UnderTest.writeToJSONDict(TestJSON);
  EXPECT_EQ(TestJSON, R"({"/:some_key1":12345, "/:some_key2":"hello"})"_json);
}