// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "MetaData/HDF5DataWriter.h"
#include "MetaData/ValueInternal.h"
#include "helpers/HDFFileTestHelper.h"
#include <functional>
#include <gtest/gtest.h>

class HDF5Data : public ::testing::Test {
public:
  void SetUp() override {
    TestFile =
        HDFFileTestHelper::createInMemoryTestFile("test-meta-data.nxs", false);
    RootGroup = TestFile->hdfGroup();
    UsedGroup = RootGroup.create_group(GroupName);
    NeXusDataset::ExtensibleDataset<int> TempDataset(
        UsedGroup, DatasetName, NeXusDataset::Mode::Create);
    UsedDataset = TempDataset.link().parent().get_dataset(DatasetName);
  }
  std::string const GroupName{"SomeGroupName"};
  std::string const DatasetName{"SomeDatasetName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
  hdf5::node::Dataset UsedDataset;
};

TEST_F(HDF5Data, IntAttributeWritten) {
  std::string Name{"someName"};
  int const Value{42};
  MetaData::basicAttributeWriter<int>(UsedGroup, Name, Value);
  ASSERT_TRUE(UsedGroup.attributes.exists(Name));
  auto Attribute = UsedGroup.attributes[Name];
  int ReadInto{0};
  Attribute.read(ReadInto);
  EXPECT_EQ(Value, ReadInto);
}

TEST_F(HDF5Data, IntDatasetWritten) {
  std::string Name{"someName"};
  int const Value{42};
  MetaData::basicDatasetWriter<int>(UsedGroup, Name, Value);
  ASSERT_TRUE(UsedGroup.has_dataset(Name));
  auto Dataset = UsedGroup.get_dataset(Name);
  std::vector<int> ReadInto;
  ReadInto.resize(1);
  Dataset.read(ReadInto);
  EXPECT_EQ(Value, ReadInto.at(0));
}

TEST_F(HDF5Data, StringAttributeWritten) {
  std::string Name{"someName"};
  std::string const Value{"hello"};
  MetaData::basicAttributeWriter<std::string>(UsedGroup, Name, Value);
  ASSERT_TRUE(UsedGroup.attributes.exists(Name));
  auto Attribute = UsedGroup.attributes[Name];
  std::string ReadInto;
  Attribute.read(ReadInto);
  EXPECT_EQ(Value, ReadInto);
}

TEST_F(HDF5Data, StringDatasetWritten) {
  std::string Name{"someName"};
  std::string const Value{"hello"};
  MetaData::basicStringDatasetWriter(UsedGroup, Name, Value);
  ASSERT_TRUE(UsedGroup.has_dataset(Name));
  auto Dataset = UsedGroup.get_dataset(Name);
  std::vector<std::string> ReadInto;
  ReadInto.resize(1);
  Dataset.read(ReadInto);
  EXPECT_EQ(Value, ReadInto.at(0));
}

TEST_F(HDF5Data, GroupStringDataDestination) {
  std::string Name{"someName"};
  MetaData::Value<int> TestDataset(fmt::format("/{}", GroupName), Name,
                                   MetaData::basicDatasetWriter<int>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  TestDataset.setValue(Value);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  ASSERT_TRUE(UsedGroup.has_dataset(Name));
  auto Dataset = UsedGroup.get_dataset(Name);
  std::vector<int> ReadInto;
  ReadInto.resize(1);
  Dataset.read(ReadInto);
  EXPECT_EQ(Value, ReadInto.at(0));
}

TEST_F(HDF5Data, GroupStringDataDestinationWritesAttributes) {
  std::string Name{"datasetWithAttribute"};
  MetaData::Value<int> TestDataset(fmt::format("/{}", GroupName), Name,
                                   MetaData::basicDatasetWriter<int>,
                                   MetaData::basicAttributeWriter<std::string>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  std::string const AttributeKey{"someKey"};
  std::string const AttributeValue{"someValue"};
  TestDataset.setValue(Value);
  TestDataset.setAttribute(AttributeKey, AttributeValue);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  ASSERT_TRUE(UsedGroup.has_dataset(Name));
  auto Dataset = UsedGroup.get_dataset(Name);
  ASSERT_TRUE(Dataset.attributes.exists(AttributeKey));
  auto Attribute = Dataset.attributes[AttributeKey];
  std::string ReadInto{};
  Attribute.read(ReadInto);
  EXPECT_EQ(AttributeValue, ReadInto);
}

TEST_F(HDF5Data, GroupNodeDataDestination) {
  std::string Name{"someName"};
  MetaData::Value<int> TestDataset(UsedGroup, Name,
                                   MetaData::basicDatasetWriter<int>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  TestDataset.setValue(Value);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  ASSERT_TRUE(UsedGroup.has_dataset(Name));
  auto Dataset = UsedGroup.get_dataset(Name);
  std::vector<int> ReadInto;
  ReadInto.resize(1);
  Dataset.read(ReadInto);
  EXPECT_EQ(Value, ReadInto.at(0));
}

TEST_F(HDF5Data, GroupStringAttributeDestination) {
  std::string Name{"someName"};
  MetaData::Value<int> TestDataset(fmt::format("/{}", GroupName), Name,
                                   MetaData::basicAttributeWriter<int>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  TestDataset.setValue(Value);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  ASSERT_TRUE(UsedGroup.attributes.exists(Name));
  auto Attribute = UsedGroup.attributes[Name];
  int ReadInto{0};
  Attribute.read(ReadInto);
  EXPECT_EQ(Value, ReadInto);
}

TEST_F(HDF5Data, GroupNodeAttributeDestination) {
  std::string Name{"someName"};
  MetaData::Value<int> TestDataset(UsedGroup, Name,
                                   MetaData::basicAttributeWriter<int>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  TestDataset.setValue(Value);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  ASSERT_TRUE(UsedGroup.attributes.exists(Name));
  auto Attribute = UsedGroup.attributes[Name];
  int ReadInto{0};
  Attribute.read(ReadInto);
  EXPECT_EQ(Value, ReadInto);
}

TEST_F(HDF5Data, DatasetDatasetDestinationFailure) {
  std::string Name{"someName"};
  MetaData::Value<int> TestDataset(UsedDataset, Name,
                                   MetaData::basicDatasetWriter<int>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  TestDataset.setValue(Value);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  // We should probably/maybe (somehow) check that we got an error here
}

TEST_F(HDF5Data, GroupNodeDataDestinationFailure) {
  std::string Name{"someName"};
  MetaData::Value<int> TestDataset("/SomeGroupThatDoesNotExist", Name,
                                   MetaData::basicDatasetWriter<int>);
  MetaData::Tracker UsedTracker;
  int const Value{42};
  TestDataset.setValue(Value);
  UsedTracker.registerMetaData(TestDataset);
  UsedTracker.writeToHDF5File(RootGroup);
  // We should probably/maybe (somehow) check that we got an error here
}
