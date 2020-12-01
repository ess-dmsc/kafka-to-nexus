// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "WriterModuleConfig/Field.h"
#include "WriterModuleConfig/FieldHandler.h"
#include "helpers/StubWriterModule.h"
#include <gtest/gtest.h>

using namespace WriterModuleConfig;

class FieldStandIn : public FieldBase {
  FieldStandIn(std::string Key);
};

class FieldHandlerFixture : public ::testing::Test {
public:
  void SetUp() override { Writer = std::make_unique<StubWriterModule>(); }
  std::unique_ptr<StubWriterModule> Writer;
};

TEST_F(FieldHandlerFixture, SingleFieldSingleKey) {
  std::string TestKey{"some_key"};
  int DefaultValue{42};
  int NewValue{55};
  Field<int> TestField(Writer.get(), TestKey, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format("{{\"{}\":{}}}", TestKey, NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.getValue(), NewValue);
}

TEST_F(FieldHandlerFixture, SingleFieldMultipleKeys) {
  std::string TestKey1{"some_key"};
  std::string TestKey2{"some_other_key"};
  int DefaultValue{42};
  int NewValue{55};
  Field<int> TestField(Writer.get(), {TestKey1, TestKey2}, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format("{{\"{}\":{}}}", TestKey2, NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.getValue(), NewValue);
}

TEST_F(FieldHandlerFixture, MultipleFieldsMultipleKeys) {
  std::string TestKey11{"some_key"};
  std::string TestKey12{"some_other_key"};
  std::string TestKey21{"some_third_key"};
  int DefaultValue1{42};
  double DefaultValue2{3.333};
  double NewValue{55.546};
  Field<int> TestField1(Writer.get(), {TestKey11, TestKey12}, DefaultValue1);
  Field<double> TestField2(Writer.get(), TestKey21, DefaultValue2);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField1);
  UnderTest.registerField(&TestField2);
  auto TestStr = fmt::format("{{\"{}\":{}}}", TestKey21, NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField2.getValue(), NewValue);
  EXPECT_EQ(TestField1.getValue(), DefaultValue1);
}

TEST_F(FieldHandlerFixture, WrongKey) {
  std::string TestKey{"some_key"};
  int DefaultValue{42};
  double NewValue{55.546};
  Field<int> TestField(Writer.get(), TestKey, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format("{{\"{}\":{}}}", "wrong_key", NewValue);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.getValue(), DefaultValue);
}

TEST_F(FieldHandlerFixture, WrongKeyButFieldRequired) {
  std::string TestKey{"some_key"};
  RequiredField<int> TestField(Writer.get(), TestKey);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format("{{\"{}\":{}}}", "wrong_key", 4564);
  EXPECT_THROW(UnderTest.processConfigData(TestStr), std::runtime_error);
}

TEST_F(FieldHandlerFixture, CorrectKeyButWrongType) {
  std::string TestKey{"some_key"};
  int DefaultValue{1};
  Field<int> TestField(Writer.get(), TestKey, DefaultValue);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format("{{\"{}\":\"{}\"}}", TestKey, "some_string");
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField.getValue(), DefaultValue);
}

TEST_F(FieldHandlerFixture, CorrectKeyAndWrongTypeAndRequriedField) {
  std::string TestKey{"some_key"};
  RequiredField<int> TestField(Writer.get(), TestKey);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField);
  auto TestStr = fmt::format("{{\"{}\":\"{}\"}}", TestKey, "some_string");
  EXPECT_THROW(UnderTest.processConfigData(TestStr), std::runtime_error);
}

TEST_F(FieldHandlerFixture, IdenticalKeys) {
  std::string TestKey{"some_key"};
  Field<int> TestField1(Writer.get(), TestKey, 1);
  Field<int> TestField2(Writer.get(), TestKey, 2);
  FieldHandler UnderTest;
  UnderTest.registerField(&TestField1);
  UnderTest.registerField(&TestField2);
  auto TestStr = fmt::format("{{\"{}\":{}}}", TestKey, 3);
  UnderTest.processConfigData(TestStr);
  EXPECT_EQ(TestField1.getValue(), 1);
  EXPECT_EQ(TestField2.getValue(), 3);
}