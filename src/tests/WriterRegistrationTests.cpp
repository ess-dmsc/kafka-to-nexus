#include "../json.h"
#include <HDFWriterModule.h>
#include <gtest/gtest.h>
#include "tests/helpers/StubWriterModule.h"

using namespace FileWriter;

using ModuleFactory = HDFWriterModuleRegistry::ModuleFactory;

class WriterRegistrationTest : public ::testing::Test {
public:
  void SetUp() override {
    std::map<std::string, ModuleFactory> &WriterFactories =
        HDFWriterModuleRegistry::getFactories();
    WriterFactories.clear();
  };
};

TEST_F(WriterRegistrationTest, SimpleRegistration) {
  std::map<std::string, ModuleFactory> &Factories =
      HDFWriterModuleRegistry::getFactories();
  std::string TestKey("temp");
  EXPECT_EQ(Factories.size(), 0u);
  { HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey); }
  EXPECT_EQ(Factories.size(), 1u);
  EXPECT_NE(Factories.find(TestKey), Factories.end());
}

TEST_F(WriterRegistrationTest, SameKeyRegistration) {
  std::string TestKey("temp");
  { HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey); }
  EXPECT_THROW(
      HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyTooShort) {
  std::string TestKey("tem");
  EXPECT_THROW(
      HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, KeyTooLong) {
  std::string TestKey("tempp");
  EXPECT_THROW(
      HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey),
      std::runtime_error);
}

TEST_F(WriterRegistrationTest, StrKeyFound) {
  std::string TestKey("t3mp");
  { HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey); }
  EXPECT_NE(HDFWriterModuleRegistry::find(TestKey), nullptr);
}

TEST_F(WriterRegistrationTest, StrKeyNotFound) {
  std::string TestKey("t3mp");
  { HDFWriterModuleRegistry::Registrar<StubWriterModule> RegisterIt(TestKey); }
  std::string FailKey("trump");
  EXPECT_THROW(HDFWriterModuleRegistry::find(FailKey), std::out_of_range);
}
