// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "JobCreator.h"
#include "ModuleHDFInfo.h"
#include "WriterModule/f142/f142_Writer.h"
#include "WriterModule/hs00/hs00_Writer.h"
#include "WriterRegistrar.h"
#include "helpers/HDFFileTestHelper.h"
#include <gtest/gtest.h>

class JobCreator : public ::testing::Test {
public:
  void SetUp() override {
    WriterModule::Registry::clear();
    WriterModule::Registry::Registrar<WriterModule::f142::f142_Writer>
        RegisterIt1("f142", "f142");
    WriterModule::Registry::Registrar<WriterModule::hs00::hs00_Writer>
        RegisterIt2("hs00", "hs00");
  }
};

TEST_F(JobCreator, ExtractStreamSettingsEmptyModule) {
  std::string ModuleId{""};
  std::string HDF5Parent{"/entry/"};
  std::string Config{R""({
              "dtype": "double",
              "source": "my_test_pv",
              "topic": "my_test_topic"})""};
  ModuleHDFInfo TestConfig{ModuleId, HDF5Parent, Config};
  EXPECT_THROW(
      FileWriter::extractModuleInformationFromJsonForSource(TestConfig),
      std::runtime_error);
}

TEST_F(JobCreator, ExtractStreamSettings) {
  std::string ModuleId{"f142"};
  std::string HDF5Parent{"/entry/"};
  std::string Config{R""({
              "dtype": "double",
              "source": "my_test_pv",
  "topic": "my_test_topic"})""};
  ModuleHDFInfo TestConfig{ModuleId, HDF5Parent, Config};
  auto Result =
      FileWriter::extractModuleInformationFromJsonForSource(TestConfig);
  EXPECT_EQ(Result.Module, ModuleId);
  EXPECT_EQ(Result.Topic, "my_test_topic");
  EXPECT_EQ(Result.Source, "my_test_pv");
  EXPECT_EQ(nlohmann::json::parse(Result.ConfigStreamJson),
            nlohmann::json::parse(Config));
}

TEST_F(JobCreator, Generatef142Writer) {
  std::string ModuleId{"f142"};
  std::string HDF5Parent{"/entry/"};
  std::string Config{R""({
              "dtype": "double",
              "source": "my_test_pv",
  "topic": "my_test_topic"})""};
  ModuleHDFInfo TestConfig{ModuleId, HDF5Parent, Config};
  auto Result =
      FileWriter::extractModuleInformationFromJsonForSource(TestConfig);
  auto WriterInstance = FileWriter::generateWriterInstance(Result);
  EXPECT_TRUE(dynamic_cast<WriterModule::f142::f142_Writer *>(
                  WriterInstance.get()) != nullptr);
}

TEST_F(JobCreator, GenerateWriterFailWithBadId) {
  std::string ModuleId{"bad_module"};
  std::string HDF5Parent{"/entry/"};
  std::string Config{R""({
              "dtype": "double",
              "source": "my_test_pv",
  "topic": "my_test_topic"})""};
  ModuleHDFInfo TestConfig{ModuleId, HDF5Parent, Config};
  auto Result =
      FileWriter::extractModuleInformationFromJsonForSource(TestConfig);
  EXPECT_THROW(FileWriter::generateWriterInstance(Result), std::runtime_error);
}

TEST_F(JobCreator, GenerateWriterFailWithMissingFields) {
  std::string ModuleId{"hs00"};
  std::string HDF5Parent{"/entry/"};
  std::string Config{R""({
              "source": "my_test_pv",
  "topic": "my_test_topic"})""};
  // Fields are missing in the JSON structure above for the hs00 module
  ModuleHDFInfo TestConfig{ModuleId, HDF5Parent, Config};
  auto Result =
      FileWriter::extractModuleInformationFromJsonForSource(TestConfig);
  EXPECT_THROW(FileWriter::generateWriterInstance(Result), std::runtime_error);
}

TEST_F(JobCreator, SetWriterAttributes) {
  auto TestFile =
      HDFFileTestHelper::createInMemoryTestFile("testFile.hdf5", false);
  std::string ModuleId{"f142"};
  std::string HDF5Parent{"/entry/"};
  std::string Config{R""({
              "dtype": "double",
              "source": "my_test_pv",
  "topic": "my_test_topic"})""};
  ModuleHDFInfo TestConfig{ModuleId, HDF5Parent, Config};
  auto StreamInfo =
      FileWriter::extractModuleInformationFromJsonForSource(TestConfig);
  auto WriterModule = FileWriter::generateWriterInstance(StreamInfo);
  StreamInfo.WriterModule = std::move(WriterModule);
  auto RootGroup = TestFile->hdfGroup();
  RootGroup.create_group("entry");
  FileWriter::setWriterHDFAttributes(RootGroup, StreamInfo);
  ASSERT_TRUE(RootGroup.get_group("entry").attributes.exists("topic"));
  std::string TempString;
  RootGroup.get_group("entry").attributes["topic"].read(TempString);
  EXPECT_EQ(TempString, "my_test_topic");
  ASSERT_TRUE(RootGroup.get_group("entry").attributes.exists("source"));
  RootGroup.get_group("entry").attributes["source"].read(TempString);
  EXPECT_EQ(TempString, "my_test_pv");
  ASSERT_TRUE(RootGroup.get_group("entry").attributes.exists("writer_module"));
  RootGroup.get_group("entry").attributes["writer_module"].read(TempString);
  EXPECT_EQ(TempString, ModuleId);
  ASSERT_TRUE(RootGroup.get_group("entry").attributes.exists("NX_class"));
  RootGroup.get_group("entry").attributes["NX_class"].read(TempString);
  EXPECT_EQ(TempString, "NXlog");
}

TEST(ExtractMdat, ExtractsAllMdatModulesFromModuleList) {
  std::vector<ModuleHDFInfo> ModuleList{
      {"not mdat", ":: parent ::", ":: stream ::"},
      {"mdat", ":: parent ::", ":: stream ::"},
      {"not mdat", ":: parent ::", ":: stream ::"},
      {"mdat", ":: parent ::", ":: stream ::"},
      {"not mdat", ":: parent ::", ":: stream ::"}};

  auto MdatModules = FileWriter::extractMdatModules(ModuleList);

  ASSERT_EQ(ModuleList.size(), static_cast<size_t>(3));
  ASSERT_EQ(MdatModules.size(), static_cast<size_t>(2));
  std::for_each(
      MdatModules.cbegin(), MdatModules.cend(),
      [](auto const &Module) { ASSERT_EQ(Module.WriterModule, "mdat"); });

  std::for_each(ModuleList.cbegin(), ModuleList.cend(), [](auto const &Module) {
    ASSERT_NE(Module.WriterModule, "mdat");
  });
}
