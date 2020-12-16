// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "HDFAttributes.h"
#include <gtest/gtest.h>
#include "helpers/HDFFileTestHelper.h"
#include "TimeUtility.h"

class HDFAttributesTest : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(GroupName);
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string GroupName{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

TEST_F(HDFAttributesTest, WriteIntAttr) {
  std::string AttributeName{"some_attribute"};
  int AttributeValue{42};
  HDFAttributes::writeAttribute(UsedGroup, AttributeName, AttributeValue);
  ASSERT_TRUE(UsedGroup.attributes.exists(AttributeName));
  int TempValue;
  UsedGroup.attributes[AttributeName].read(TempValue);
  EXPECT_EQ(TempValue, AttributeValue);
}

TEST_F(HDFAttributesTest, WriteStrAttr) {
  std::string AttributeName{"some_attribute"};
  std::string AttributeValue{"hello"};
  HDFAttributes::writeAttribute(UsedGroup, AttributeName, AttributeValue);
  ASSERT_TRUE(UsedGroup.attributes.exists(AttributeName));
  std::string TempValue;
  UsedGroup.attributes[AttributeName].read(TempValue);
  EXPECT_EQ(TempValue, AttributeValue);
}

TEST_F(HDFAttributesTest, WriteVectorAttr) {
  std::string AttributeName{"some_attribute"};
  std::vector<double> AttributeValue{1.1, 2.2, 3.3};
  HDFAttributes::writeAttribute(UsedGroup, AttributeName, AttributeValue);
  ASSERT_TRUE(UsedGroup.attributes.exists(AttributeName));
  std::vector<double> TempValue(3);
  UsedGroup.attributes[AttributeName].read(TempValue);
  EXPECT_EQ(TempValue, AttributeValue);
}

TEST_F(HDFAttributesTest, WriteDateTimeAttr) {
  std::string AttributeName{"some_attribute"};
  time_point AttributeValue{system_clock::now()};
  std::string TimeString{toUTCDateTime(AttributeValue)};
  HDFAttributes::writeAttribute(UsedGroup, AttributeName, AttributeValue);
  ASSERT_TRUE(UsedGroup.attributes.exists(AttributeName));
  std::string TempValue;
  UsedGroup.attributes[AttributeName].read(TempValue);
  EXPECT_EQ(TempValue, TimeString);
}

TEST_F(HDFAttributesTest, WriteVectorStrAttr) {
  std::string AttributeName{"some_attribute"};
  std::vector<std::string> AttributeValue{"hello", "hi", "lets go"};
  HDFAttributes::writeAttribute(UsedGroup, AttributeName, AttributeValue);
  ASSERT_TRUE(UsedGroup.attributes.exists(AttributeName));
  std::vector<std::string> TempValue(3);
  UsedGroup.attributes[AttributeName].read(TempValue);
  EXPECT_EQ(TempValue, AttributeValue);
}

TEST_F(HDFAttributesTest, WriteVectorOfVectorsAttr) {
  std::string AttributeName{"some_attribute"};
  std::vector<std::vector<int>> AttributeValue{{1,2,3}, {4,5,6}};
  HDFAttributes::writeAttribute(UsedGroup, AttributeName, AttributeValue);
  ASSERT_TRUE(UsedGroup.attributes.exists(AttributeName));
  std::vector<std::vector<int>> TempValue(2);
  TempValue[0].resize(3);
  TempValue[1].resize(3);
  UsedGroup.attributes[AttributeName].read(TempValue);
  EXPECT_EQ(TempValue, AttributeValue);
}
