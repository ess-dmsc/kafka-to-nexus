
#include <gtest/gtest.h>
#include "WriterModuleConfig/Field.h"
#include <nlohmann/json.hpp>
#include <iostream>
#include <array>
#include <vector>

using nlohmann::json;

TEST(WriterConfigField, SetWithCorrectType) {
  WriterModuleConfig::Field<int> UnderTest(nullptr, "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  UnderTest.setValue("124");
  EXPECT_EQ(UnderTest.getValue(), 124);
}

TEST(WriterConfigField, SetWithWrongType) {
  WriterModuleConfig::Field<int> UnderTest(nullptr, "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  EXPECT_THROW(UnderTest.setValue("\"hello\""), json::type_error);
}

TEST(WriterConfigField, SetWithBadJson) {
  WriterModuleConfig::Field<int> UnderTest(nullptr, "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  EXPECT_THROW(UnderTest.setValue("{3,5}"), json::parse_error);
}

TEST(WriterConfigField, SetWithRegularString) {
  WriterModuleConfig::Field<std::string> UnderTest(nullptr, "some_key", "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("\"some_string\"");
  EXPECT_EQ(UnderTest.getValue(), "some_string");
}

TEST(WriterConfigField, SetWithParseFailString) {
  WriterModuleConfig::Field<std::string> UnderTest(nullptr, "some_key", "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("{3,3}");
  EXPECT_EQ(UnderTest.getValue(), "{3,3}");
}

TEST(WriterConfigField, SetWithTypeFailString) {
  WriterModuleConfig::Field<std::string> UnderTest(nullptr, "some_key", "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("3.1345");
  EXPECT_EQ(UnderTest.getValue(), "3.1345");
}
