
#include "WriterModuleConfig/Field.h"
#include "helpers/StubWriterModule.h"
#include <array>
#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <vector>

using nlohmann::json;

class WriterConfigField : public testing::Test {
public:
  void SetUp() override { Writer = std::make_unique<StubWriterModule>(); }
  std::unique_ptr<StubWriterModule> Writer;
};

TEST_F(WriterConfigField, SetWithCorrectType) {
  WriterModuleConfig::Field<int> UnderTest(Writer.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  UnderTest.setValue("124");
  EXPECT_EQ(UnderTest.getValue(), 124);
}

TEST_F(WriterConfigField, SetWithWrongType) {
  WriterModuleConfig::Field<int> UnderTest(Writer.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  EXPECT_THROW(UnderTest.setValue("\"hello\""), json::type_error);
}

TEST_F(WriterConfigField, SetWithBadJson) {
  WriterModuleConfig::Field<int> UnderTest(Writer.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  EXPECT_THROW(UnderTest.setValue("{3,5}"), json::parse_error);
}

TEST_F(WriterConfigField, SetWithRegularString) {
  WriterModuleConfig::Field<std::string> UnderTest(Writer.get(), "some_key",
                                                   "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("\"some_string\"");
  EXPECT_EQ(UnderTest.getValue(), "some_string");
}

TEST_F(WriterConfigField, SetWithParseFailString) {
  WriterModuleConfig::Field<std::string> UnderTest(Writer.get(), "some_key",
                                                   "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("{3,3}");
  EXPECT_EQ(UnderTest.getValue(), "{3,3}");
}

TEST_F(WriterConfigField, SetWithTypeFailString) {
  WriterModuleConfig::Field<std::string> UnderTest(Writer.get(), "some_key",
                                                   "hello");
  EXPECT_EQ(UnderTest.getValue(), "hello");
  UnderTest.setValue("3.1345");
  EXPECT_EQ(UnderTest.getValue(), "3.1345");
}

TEST_F(WriterConfigField, SetTwice) {
  WriterModuleConfig::Field<int> UnderTest(Writer.get(), "some_key", 33);
  EXPECT_EQ(UnderTest.getValue(), 33);
  UnderTest.setValue("124");
  EXPECT_EQ(UnderTest.getValue(), 124);
  UnderTest.setValue("11");
  EXPECT_EQ(UnderTest.getValue(), 11);
}