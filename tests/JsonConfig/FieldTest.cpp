
#include "JsonConfig/Field.h"
#include "JsonConfig/FieldHandler.h"
#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>

using nlohmann::json;
using namespace std::string_literals;

class JsonConfigField : public testing::Test {
public:
  void SetUp() override {
    FieldHandler = std::make_unique<JsonConfig::FieldHandler>();
  }
  std::unique_ptr<JsonConfig::FieldHandler> FieldHandler;
};

TEST_F(JsonConfigField, ValueSetOnConstruction) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 456);

  EXPECT_EQ(UnderTest.get_value(), 456);
}

TEST_F(JsonConfigField, SetWithCorrectType) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);

  UnderTest.setValue(""s, "124"s);

  EXPECT_EQ(UnderTest.get_value(), 124);
}

TEST_F(JsonConfigField, SetWithWrongTypeThrows) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);

  EXPECT_THROW(UnderTest.setValue(""s, "\"hello\""s), json::type_error);
}

TEST_F(JsonConfigField, SetWithInvalidJsonThrows) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);

  EXPECT_THROW(UnderTest.setValue(""s, "{3,5}"s), json::exception);
}

TEST_F(JsonConfigField, SetStringValue) {
  JsonConfig::Field<std::string> UnderTest(FieldHandler.get(), "some_key",
                                           "hello");

  UnderTest.setValue(""s, "\"some_string\""s);

  EXPECT_EQ(UnderTest.get_value(), "some_string");
}

TEST_F(JsonConfigField, SetWithJsonObject) {
  JsonConfig::Field<nlohmann::json> UnderTest(FieldHandler.get(), "some_key",
                                              nlohmann::json::parse("[1,2]"));

  UnderTest.setValue(""s, "[2,3,4]"s);

  EXPECT_EQ(UnderTest.get_value(), nlohmann::json::parse("[2,3,4]"));
}

TEST_F(JsonConfigField, SetStringFieldWithJsonString) {
  JsonConfig::Field<std::string> UnderTest(FieldHandler.get(), "some_key",
                                           "hello");

  UnderTest.setValue(""s, "{3,3}"s);

  EXPECT_EQ(UnderTest.get_value(), "{3,3}");
}

TEST_F(JsonConfigField, SetStringFieldWithIntStoresAsString) {
  JsonConfig::Field<std::string> UnderTest(FieldHandler.get(), "some_key",
                                           "hello");

  UnderTest.setValue(""s, "3.1345"s);

  EXPECT_EQ(UnderTest.get_value(), "3.1345");
}

TEST_F(JsonConfigField, SetMultipleTimes) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), "some_key", 33);

  EXPECT_EQ(UnderTest.get_value(), 33);

  UnderTest.setValue(""s, "124"s);

  EXPECT_EQ(UnderTest.get_value(), 124);

  UnderTest.setValue(""s, "11"s);

  EXPECT_EQ(UnderTest.get_value(), 11);
}

TEST_F(JsonConfigField, SetTwoKeys) {
  JsonConfig::Field<int> UnderTest(FieldHandler.get(), {"h1", "h2"}, 33);
  std::string H1{"h1"};
  std::string H2{"h2"};
  std::vector<std::string> Comparison{H1, H2};
  EXPECT_EQ(UnderTest.getKeys(), Comparison);
}

TEST_F(JsonConfigField, SetTwoRequiredKeys) {
  JsonConfig::RequiredField<int> UnderTest(FieldHandler.get(), {"h1", "h2"});
  std::string H1{"h1"};
  std::string H2{"h2"};
  std::vector<std::string> Comparison{H1, H2};
  EXPECT_EQ(UnderTest.getKeys(), Comparison);
}
