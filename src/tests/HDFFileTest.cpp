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
    StreamHDFInfo.clear();
  }
  std::string FileName{"someFileName.hdf"};
  nlohmann::json NexusStructure{};
  std::vector<StreamHDFInfo> StreamHDFInfo;
  MetaData::TrackerPtr Tracker{};
};

TEST_F(HDFFile, FileModes) {
  FileWriter::HDFFile UnderTest{FileName, NexusStructure, StreamHDFInfo,
                                Tracker};
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
  FileWriter::HDFFile UnderTest{FileName, NexusStructure, StreamHDFInfo,
                                Tracker};
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
  EXPECT_TRUE(StreamHDFInfo.empty());
  FileWriter::HDFFile UnderTest{FileName,
                                nlohmann::json::parse(SimpleNexusStructure),
                                StreamHDFInfo, Tracker};
  EXPECT_TRUE(UnderTest.hdfGroup().has_group("entry"));
  EXPECT_EQ(StreamHDFInfo.size(), 1u);
}
