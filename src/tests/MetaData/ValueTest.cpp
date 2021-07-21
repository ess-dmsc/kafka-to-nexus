#include "MetaData/Value.h"
#include <gtest/gtest.h>
#include <string>
#include <vector>

using std::string_literals::operator""s;

TEST(MetaData, IntValue) {
  MetaData::Value<int> UnderTest{"SomeKey"};
  int TestValue{1235};
  UnderTest.setValue(TestValue);

  EXPECT_EQ(TestValue, UnderTest.getValue());
  EXPECT_EQ(UnderTest.getAsJSON(), nlohmann::json{R"({"SomeKey":1235})"_json});
}

TEST(MetaData, IntVectorValue) {
  MetaData::Value<std::vector<int>> UnderTest{"SomeKey"};
  std::vector<int> TestValue{1, 2, 3, 4};
  UnderTest.setValue(TestValue);

  EXPECT_EQ(TestValue, UnderTest.getValue());
  EXPECT_EQ(UnderTest.getAsJSON(),
            nlohmann::json{R"({"SomeKey":[1,2,3,4]})"_json});
}

TEST(MetaData, StringValue) {
  MetaData::Value<std::string> UnderTest{"SomeKey"};
  std::string TestValue{"hello"};
  UnderTest.setValue(TestValue);

  EXPECT_EQ(TestValue, UnderTest.getValue());
  EXPECT_EQ(UnderTest.getAsJSON(),
            nlohmann::json{R"({"SomeKey":"hello"})"_json});
}
