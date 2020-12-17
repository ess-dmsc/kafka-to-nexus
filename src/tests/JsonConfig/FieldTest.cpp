
#include "JsonConfig/Field.h"
#include "JsonConfig/FieldHandler.h"
#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>

using nlohmann::json;

class JsonConfigField : public testing::Test {
public:
  void SetUp() override {
    FieldHandler = std::make_unique<JsonConfig::FieldHandler>();
  }
  std::unique_ptr<JsonConfig::FieldHandler> FieldHandler;
};

TEST_F(JsonConfigField, SetWithCorrectType) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  UnderTest.setValue("124");
  EXPECT_EQ(UnderTest.getValue(), 124);
}

TEST_F(JsonConfigField, SetWithWrongType) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  EXPECT_THROW(UnderTest.setValue("\"hello\""), json::type_error);
}

TEST_F(JsonConfigField, SetWithBadJson) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  EXPECT_THROW(UnderTest.setValue("{3,5}"), json::parse_error);
}

TEST_F(JsonConfigField, SetWithRegularString) {
  JsonConfig::Field<std::string> UnderTest(FieldHandler.get(), "some_key",
                                           "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("\"some_string\"");
  EXPECT_EQ(UnderTest.getValue(), "some_string");
}

TEST_F(JsonConfigField, SetWithParseFailString) {
  JsonConfig::Field<std::string> UnderTest(FieldHandler.get(), "some_key",
                                           "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("{3,3}");
  EXPECT_EQ(UnderTest.getValue(), "{3,3}");
}

TEST_F(JsonConfigField, SetWithTypeFailString) {
  JsonConfig::Field<std::string> UnderTest(FieldHandler.get(), "some_key",
                                           "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("3.1345");
  EXPECT_EQ(UnderTest.getValue(), "3.1345");
}

TEST_F(JsonConfigField, SetTwice) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  UnderTest.setValue("124");
  EXPECT_EQ(UnderTest.getValue(), 124);
  UnderTest.setValue("11");
  EXPECT_EQ(UnderTest.getValue(), 11);
}
