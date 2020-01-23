// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "../json.h"
#include "WriterRegistrar.h"
#include "helpers/StubWriterModule.h"
#include <WriterModuleBase.h>
#include <gtest/gtest.h>

using namespace FileWriter;

using ModuleFactory = WriterModule::Registry::ModuleFactory;

class WriterRegistrationTest : public ::testing::Test {
public:
  void SetUp() override { WriterModule::Registry::clear(); };
};

TEST_F(WriterRegistrationTest, SimpleRegistration) {
  std::string TestKey("temp");
  EXPECT_EQ(WriterModule::Registry::getFactoryIdsAndNames().size(), 0u);
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(TestKey,
                                                                   "some_name");
  }
  EXPECT_EQ(WriterModule::Registry::getFactoryIdsAndNames().size(), 1u);
  EXPECT_NO_THROW(WriterModule::Registry::find(TestKey));
}

TEST_F(WriterRegistrationTest, SameKeyRegistration) {
  std::string TestKey("temp");
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(TestKey,
                                                                   "some_name");
  }
  EXPECT_THROW(WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(
                   TestKey, "some_name"),
               std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyTooShort) {
  std::string TestKey("tem");
  EXPECT_THROW(WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(
                   TestKey, "some_name"),
               std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyTooLong) {
  std::string TestKey("tempp");
  EXPECT_THROW(WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(
                   TestKey, "some_name"),
               std::runtime_error);
}

TEST_F(WriterRegistrationTest, StrKeyFound) {
  std::string TestKey("t3mp");
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(TestKey,
                                                                   "some_name");
  }
  EXPECT_NE(WriterModule::Registry::find(TestKey), nullptr);
}

TEST_F(WriterRegistrationTest, StrKeyNotFound) {
  std::string TestKey("t3mp");
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(TestKey,
                                                                   "some_name");
  }
  std::string FailKey("trump");
  EXPECT_THROW(WriterModule::Registry::find(FailKey), std::out_of_range);
}
