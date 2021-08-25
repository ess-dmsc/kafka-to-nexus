// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "MetaData/Tracker.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

TEST(MetaDataTracker, SingleValue) {
  MetaData::Tracker UnderTest;
  std::string Key1{"some_key1"};
  MetaData::Value<int> TestValue{"/", Key1};
  TestValue.setValue(12345);
  UnderTest.registerMetaData(TestValue);
  nlohmann::json TestJSON("{}"_json);
  UnderTest.writeToJSONDict(TestJSON);
  EXPECT_EQ(TestJSON, R"({"some_key1":12345})"_json);
}

TEST(MetaDataTracker, TwoValues) {
  MetaData::Tracker UnderTest;
  std::string Key1{"some_key1"};
  std::string Key2{"some_key2"};
  MetaData::Value<int> TestValue1{"/", Key1};
  TestValue1.setValue(12345);
  MetaData::Value<std::string> TestValue2{"/", Key2};
  TestValue2.setValue("hello");
  UnderTest.registerMetaData(TestValue1);
  UnderTest.registerMetaData(TestValue2);
  nlohmann::json TestJSON("{}"_json);
  UnderTest.writeToJSONDict(TestJSON);
  EXPECT_EQ(TestJSON, R"({"some_key1":12345, "some_key2":"hello"})"_json);
}