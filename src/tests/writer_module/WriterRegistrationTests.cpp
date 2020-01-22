// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "../json.h"
#include "helpers/StubWriterModule.h"
#include <WriterModuleBase.h>
#include <gtest/gtest.h>
#include "WriterRegistrar.h"

using namespace FileWriter;

using ModuleFactory = Module::Registry::ModuleFactory;

class WriterRegistrationTest : public ::testing::Test {
public:
  void SetUp() override {
    Module::Registry::clear();
  };
};

TEST_F(WriterRegistrationTest, SimpleRegistration) {
  std::string TestKey("temp");
  EXPECT_EQ(Module::Registry::getFactoryIdsAndNames().size(), 0u);
  { Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"); }
  EXPECT_EQ(Module::Registry::getFactoryIdsAndNames().size(), 1u);
  EXPECT_NO_THROW(Module::Registry::find(TestKey));
}

TEST_F(WriterRegistrationTest, SameKeyRegistration) {
  std::string TestKey("temp");
  { Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"); }
  EXPECT_THROW(
      Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyTooShort) {
  std::string TestKey("tem");
  EXPECT_THROW(
      Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyTooLong) {
  std::string TestKey("tempp");
  EXPECT_THROW(
      Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, StrKeyFound) {
  std::string TestKey("t3mp");
  { Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"); }
  EXPECT_NE(Module::Registry::find(TestKey), nullptr);
}

TEST_F(WriterRegistrationTest, StrKeyNotFound) {
  std::string TestKey("t3mp");
  { Module::Registry::Registrar<StubWriterModule> RegisterIt(TestKey, "some_name"); }
  std::string FailKey("trump");
  EXPECT_THROW(Module::Registry::find(FailKey), std::out_of_range);
}
