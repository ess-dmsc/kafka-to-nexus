// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFFile.h"
#include <filesystem>
#include <gtest/gtest.h>

class HDFFile : public ::testing::Test {
public:
  void SetUp() override {
    if (std::filesystem::exists(FileName)) {
      std::filesystem::remove(FileName);
    }
    ModuleHDFInfoList.clear();
  }
  std::filesystem::path FileName{"someFileName.hdf"};
  nlohmann::json NexusStructure{};
  std::vector<ModuleHDFInfo> ModuleHDFInfoList;
  MetaData::TrackerPtr Tracker{};
  std::filesystem::path template_path;
  bool is_legacy_writing = true;
};

TEST_F(HDFFile, FileModes) {
  FileWriter::HDFFile UnderTest{FileName, NexusStructure, ModuleHDFInfoList,
                                Tracker,  template_path,  is_legacy_writing};
  EXPECT_TRUE(UnderTest.isRegularMode());
  EXPECT_FALSE(UnderTest.isSWMRMode());
  UnderTest.openInSWMRMode();
  EXPECT_TRUE(UnderTest.isSWMRMode());
  EXPECT_FALSE(UnderTest.isRegularMode());
  UnderTest.openInRegularMode();
  EXPECT_TRUE(UnderTest.isRegularMode());
  EXPECT_FALSE(UnderTest.isSWMRMode());
}

TEST_F(HDFFile, DefaultFileAttributes) {
  FileWriter::HDFFile UnderTest{FileName, NexusStructure, ModuleHDFInfoList,
                                Tracker,  template_path,  is_legacy_writing};
  auto RootGroup = UnderTest.hdfGroup();
  EXPECT_TRUE(RootGroup.attributes.exists("HDF5_Version"));
  EXPECT_TRUE(RootGroup.attributes.exists("creator"));
  ASSERT_TRUE(RootGroup.attributes.exists("file_name"));
  std::string TempString;
  RootGroup.attributes["file_name"].read(TempString);
  EXPECT_EQ(TempString, FileName);
  EXPECT_TRUE(RootGroup.attributes.exists("file_time"));
  EXPECT_FALSE(
      RootGroup.attributes.exists("some_attribute_that_does_not_exist"));
}

TEST_F(HDFFile, SimpleNexusStructure) {
  std::string SimpleNexusStructure = R""({
      "children": [
        {
          "name": "entry",
          "type": "group",
          "attributes": [
            {
              "name": "NX_class",
              "dtype": "string",
              "values": "NXentry"
            }
          ],
          "children": [
            {
              "module": "f142",
              "config": {
                "dtype": "double",
                "source": "my_test_pv",
                "topic": "my_test_topic"
              }
            }
          ]
        }
      ]
  })"";
  EXPECT_TRUE(ModuleHDFInfoList.empty());
  FileWriter::HDFFile UnderTest{
      FileName,          nlohmann::json::parse(SimpleNexusStructure),
      ModuleHDFInfoList, Tracker,
      template_path,     is_legacy_writing};
  EXPECT_TRUE(UnderTest.hdfGroup().has_group("entry"));
  EXPECT_EQ(ModuleHDFInfoList.size(), 1u);
}

TEST_F(HDFFile, WithTemplatePathNoInstrumentName) {
  std::string SimpleNexusStructure = R""({
      "children": [
        {
          "name": "entry",
          "type": "group",
          "attributes": [
            {
              "name": "NX_class",
              "dtype": "string",
              "values": "NXentry"
            }
          ],
          "children": [
            {
              "module": "f142",
              "config": {
                "dtype": "double",
                "source": "my_test_pv",
                "topic": "my_test_topic"
              }
            }
          ]
        }
      ]
  })"";
  template_path = "templateFile.hdf";
  is_legacy_writing = true;

  FileWriter::HDFFile UnderTest{
      FileName,          nlohmann::json::parse(SimpleNexusStructure),
      ModuleHDFInfoList, Tracker,
      template_path,     is_legacy_writing};
  EXPECT_TRUE(UnderTest.hdfGroup().attributes.exists("HDF5_Version"));
  EXPECT_TRUE(UnderTest.hdfGroup().attributes.exists("creator"));
  EXPECT_FALSE(UnderTest.hdfGroup().attributes.exists("dynamic_version"));
  EXPECT_FALSE(UnderTest.hdfGroup().attributes.exists("template_version"));
}

TEST_F(HDFFile, NoTemplatePathWithInstrumentName) {
  std::string SimpleNexusStructure = R""({
      "dynamic_version": "1.0.0",
      "template_version": "1.0.0",
      "children": [
        {
          "name": "entry",
          "type": "group",
          "attributes": [
            {
              "name": "NX_class",
              "dtype": "string",
              "values": "NXentry"
            }
          ],
          "children": [
            {
              "module": "f142",
              "config": {
                "dtype": "double",
                "source": "my_test_pv",
                "topic": "my_test_topic"
              }
            }
          ]
        }
      ]
  })"";
  template_path = "";
  is_legacy_writing = false;

  FileWriter::HDFFile UnderTest{
      FileName,          nlohmann::json::parse(SimpleNexusStructure),
      ModuleHDFInfoList, Tracker,
      template_path,     is_legacy_writing};
  EXPECT_FALSE(UnderTest.hdfGroup().attributes.exists("HDF5_Version"));
  EXPECT_FALSE(UnderTest.hdfGroup().attributes.exists("creator"));
  EXPECT_FALSE(UnderTest.hdfGroup().attributes.exists("dynamic_version"));
  EXPECT_TRUE(UnderTest.hdfGroup().attributes.exists("template_version"));
}

TEST_F(HDFFile, WithTemplatePathWithInstrumentName) {
  std::string SimpleNexusStructure = R""({
      "dynamic_version": "1.0.0",
      "template_version": "1.0.0",
      "children": [
        {
          "name": "entry",
          "type": "group",
          "attributes": [
            {
              "name": "NX_class",
              "dtype": "string",
              "values": "NXentry"
            }
          ],
          "children": [
            {
              "module": "f142",
              "config": {
                "dtype": "double",
                "source": "my_test_pv",
                "topic": "my_test_topic"
              }
            }
          ]
        }
      ]
  })"";
  template_path = "templateFile.hdf";
  is_legacy_writing = false;

  try {
    FileWriter::HDFFile UnderTest{
        FileName,          nlohmann::json::parse(SimpleNexusStructure),
        ModuleHDFInfoList, Tracker,
        template_path,     is_legacy_writing};
    FAIL() << "Expected std::exception";
  } catch (const std::exception &e) {
    std::string error_message = e.what();
    EXPECT_EQ(error_message, "filesystem error: cannot copy: No such file or "
                             "directory [templateFile.hdf] [someFileName.hdf]");
  } catch (...) {
    FAIL() << "Expected std::exception";
  }
}
