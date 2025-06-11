// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "JsonConfig/FieldHandler.h"
#include "JsonConfig/Field.h"
#include <gtest/gtest.h>

using namespace JsonConfig;

class FieldStandIn : public FieldBase {
  FieldStandIn(std::string Key);
};

class FieldHandlerFixture : public ::testing::Test {
public:
  void SetUp() override {
    Handler = std::make_unique<JsonConfig::FieldHandler>();
  }
  std::unique_ptr<JsonConfig::FieldHandler> Handler;
};

TEST_F(FieldHandlerFixture, SingleFieldSingleKey) {
  std::string TestKey{"some_key"};
  int DefaultValue{42};
  int NewValue{55};
  Field<int> TestField(Handler.get(), TestKey, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format(R"({{"{}":{}}})", TestKey, NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.get_value(), NewValue);
}

TEST_F(FieldHandlerFixture, SingleFieldMultipleKeys) {
  std::string TestKey1{"some_key"};
  std::string TestKey2{"some_other_key"};
  int DefaultValue{42};
  int NewValue{55};
  Field<int> TestField(Handler.get(), {TestKey1, TestKey2}, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format(R"({{"{}":{}}})", TestKey2, NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.get_value(), NewValue);
}

TEST_F(FieldHandlerFixture, MultipleFieldsMultipleKeys) {
  std::string TestKey11{"some_key"};
  std::string TestKey12{"some_other_key"};
  std::string TestKey21{"some_third_key"};
  int DefaultValue1{42};
  double DefaultValue2{3.333};
  double NewValue{55.546};
  Field<int> TestField1(Handler.get(), {TestKey11, TestKey12}, DefaultValue1);
  Field<double> TestField2(Handler.get(), TestKey21, DefaultValue2);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField1);
  UnderTest.registerField(&TestField2);
  auto TestStr = fmt::format(R"({{"{}":{}}})", TestKey21, NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField2.get_value(), NewValue);
  EXPECT_EQ(TestField1.get_value(), DefaultValue1);
}

TEST_F(FieldHandlerFixture, WrongKey) {
  std::string TestKey{"some_key"};
  int DefaultValue{42};
  double NewValue{55.546};
  Field<int> TestField(Handler.get(), TestKey, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format(R"({{"{}":{}}})", "wrong_key", NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.get_value(), DefaultValue);
}

TEST_F(FieldHandlerFixture, WrongKeyButFieldRequired) {
  std::string TestKey{"some_key"};
  RequiredField<int> TestField(Handler.get(), TestKey);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format(R"({{"{}":{}}})", "wrong_key", 4564);
  EXPECT_THROW(UnderTest.processConfigData(TestStr), std::runtime_error);
}

TEST_F(FieldHandlerFixture, CorrectKeyButWrongType) {
  std::string TestKey{"some_key"};
  int DefaultValue{1};
  Field<int> TestField(Handler.get(), TestKey, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format(R"({{"{}":"{}"}})", TestKey, "some_string");
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.get_value(), DefaultValue);
}

TEST_F(FieldHandlerFixture, CorrectKeyAndWrongTypeAndRequriedField) {
  std::string TestKey{"some_key"};
  RequiredField<int> TestField(Handler.get(), TestKey);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format(R"({{"{}":"{}"}})", TestKey, "some_string");
  EXPECT_THROW(UnderTest.processConfigData(TestStr), std::runtime_error);
}

TEST_F(FieldHandlerFixture, IdenticalKeys) {
  std::string TestKey{"some_key"};
  Field<int> TestField1(Handler.get(), TestKey, 1);
  Field<int> TestField2(Handler.get(), TestKey, 2);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField1);
  UnderTest.registerField(&TestField2);
  auto TestStr = fmt::format(R"({{"{}":{}}})", TestKey, 3);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField1.get_value(), 1);
  EXPECT_EQ(TestField2.get_value(), 3);
}
