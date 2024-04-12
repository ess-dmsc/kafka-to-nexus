// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "WriterRegistrar.h"
#include "helpers/StubWriterModule.h"
#include <gtest/gtest.h>

using namespace FileWriter;

using ModuleFactory = WriterModule::Registry::ModuleFactory;

class WriterRegistrationTest : public ::testing::Test {
public:
  void SetUp() override { WriterModule::Registry::clear(); };
};

TEST_F(WriterRegistrationTest, SimpleRegistration) {
  std::string TestKey("temp");
  std::string TestName("SomeName");
  EXPECT_EQ(WriterModule::Registry::getFactoryIdsAndNames().size(), 0u);
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(TestKey,
                                                                   TestName);
  }
  EXPECT_EQ(WriterModule::Registry::getFactoryIdsAndNames().size(), 1u);
  EXPECT_NO_THROW(WriterModule::Registry::find(TestName));
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

TEST_F(WriterRegistrationTest, SameNameRegistration) {
  std::string TestKey("temp");
  std::string TestName("some_name");
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(TestKey,
                                                                   TestName);
  }
  EXPECT_THROW(WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(
                   "tmp2", TestName),
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

TEST_F(WriterRegistrationTest, HashKeyFound) {
  std::string UsedKey("t3mp");
  std::string UsedName("some_module_name");
  auto UsedHash =
      WriterModule::Registry::getWriterModuleHash({UsedKey, UsedName});
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(UsedKey,
                                                                   UsedName);
  }
  EXPECT_NE(WriterModule::Registry::find(UsedHash).first, nullptr);
}

TEST_F(WriterRegistrationTest, HashKeyNotFoundThrows) {
  std::string UsedKey("t3mp");
  std::string WrongKey("1234");
  std::string UsedName("some_module_name");
  auto UsedHash =
      WriterModule::Registry::getWriterModuleHash({WrongKey, UsedName});
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(UsedKey,
                                                                   UsedName);
  }
  EXPECT_THROW(WriterModule::Registry::find(UsedHash), std::out_of_range);
}

TEST_F(WriterRegistrationTest, FactoryIdsAndNamesRegisterCorrectly) {
  std::map<std::string, std::string> NamesAndIds{
      {"1234", "name 1"}, {"2345", "name 2"}, {"3456", "name 3"}};
  for (auto &Itm : NamesAndIds) {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(Itm.first,
                                                                   Itm.second);
  }
  auto RegisterdModules = WriterModule::Registry::getFactoryIdsAndNames();
  EXPECT_EQ(RegisterdModules.size(), NamesAndIds.size());
  for (auto &CItm : RegisterdModules) {
    EXPECT_EQ(CItm.Name, NamesAndIds[CItm.Id]);
  }
}

TEST_F(WriterRegistrationTest, GeneratedHash) {
  using WriterModule::Registry::getWriterModuleHash;
  auto Key1 = "tst1";
  auto Key2 = "tst2";
  auto Name1 = "Some name 1";
  auto Name2 = "Some name 2";
  EXPECT_NE(getWriterModuleHash({Key1, Name1}),
            getWriterModuleHash({Key1, Name2}));
  EXPECT_NE(getWriterModuleHash({Key1, Name1}),
            getWriterModuleHash({Key2, Name1}));
  EXPECT_EQ(getWriterModuleHash({Key1, Name1}),
            getWriterModuleHash({Key1, Name1}));
}

TEST_F(WriterRegistrationTest, FindModuleUsingName) {
  std::string UsedKey("t3mp");
  std::string UsedName("some_module_name");
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(UsedKey,
                                                                   UsedName);
  }
  EXPECT_NE(WriterModule::Registry::find(UsedName).first, nullptr);
}

TEST_F(WriterRegistrationTest, FindModuleThrowsForUnknownName) {
  std::string UsedKey("t3mp");
  std::string UsedName("some_module_name");
  {
    WriterModule::Registry::Registrar<StubWriterModule> RegisterIt(UsedKey,
                                                                   UsedName);
  }
  EXPECT_THROW(WriterModule::Registry::find("Some other name"),
               std::out_of_range);
}
