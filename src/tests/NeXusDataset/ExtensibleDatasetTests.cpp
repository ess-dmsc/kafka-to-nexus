// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "NeXusDataset/ExtensibleDataset.h"
#include <gtest/gtest.h>
#include <h5cpp/dataspace/simple.hpp>
#include <h5cpp/datatype/type_trait.hpp>
#include <h5cpp/hdf5.hpp>

class DatasetCreation : public ::testing::Test {
public:
  void SetUp() override {
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE);
    RootGroup = File.root();
  };

  void TearDown() override { File.close(); };
  std::string TestFileName{"DatasetCreationTestFile.hdf5"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
};

TEST_F(DatasetCreation, MultiDimOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, {10, 10}, {ChunkSize});
  }
  EXPECT_NO_THROW(NeXusDataset::MultiDimDataset<int> ReOpened(
      RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(DatasetCreation, MultiDimDArrChunkSizeAlt1) {
  NeXusDataset::MultiDimDataset<int> ADValues(
      RootGroup, NeXusDataset::Mode::Create, {10, 10}, {5, 6, 4, 3});
  auto ChunkDims = ADValues.creation_list().chunk();
  auto ExpectedDims = hdf5::Dimensions{1024, 10, 10};
  EXPECT_EQ(ChunkDims, ExpectedDims);
}

TEST_F(DatasetCreation, MultiDimDArrChunkSizeAlt2) {
  NeXusDataset::MultiDimDataset<int> ADValues(
      RootGroup, NeXusDataset::Mode::Create, {10, 10}, {10, 10, 10});
  auto ChunkDims = ADValues.creation_list().chunk();
  auto ExpectedDims = hdf5::Dimensions{10, 10, 10};
  EXPECT_EQ(ChunkDims, ExpectedDims);
}

TEST_F(DatasetCreation, MultiDimReOpen) {
  size_t ChunkSize{256};
  {
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, {10, 10}, {ChunkSize});
  }
  EXPECT_NO_THROW(NeXusDataset::MultiDimDataset<int>(
      RootGroup, NeXusDataset::Mode::Open, {10, 10}, {ChunkSize}));
}

TEST_F(DatasetCreation, MultiDimCreateFail) {
  // Fail if shape is not provided
  EXPECT_THROW(NeXusDataset::MultiDimDataset<int> ReOpened(
                   RootGroup, NeXusDataset::Mode::Create),
               std::runtime_error);
}

TEST_F(DatasetCreation, MultiDimUnknownMode) {
  EXPECT_THROW(NeXusDataset::MultiDimDataset<int> ReOpened(
                   RootGroup, NeXusDataset::Mode(-43536)),
               std::runtime_error);
}

TEST_F(DatasetCreation, MultiDimCreationMaxSize) {
  hdf5::Dimensions DatasetDimensions{10, 10};
  {
    size_t ChunkSize{256};
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {ChunkSize});
  }
  NeXusDataset::MultiDimDataset<int> ReOpened(RootGroup,
                                              NeXusDataset::Mode::Open);
  auto DataSpace = ReOpened.dataspace();
  hdf5::dataspace::Simple SomeSpace(DataSpace);
  auto MaxDims = SomeSpace.maximum_dimensions();
  for (auto i : MaxDims) {
    EXPECT_EQ(
        i,
        H5S_UNLIMITED); // Due to bug in h5cpp I am not using Simple::UNLIMITED
  }
}

TEST_F(DatasetCreation, MultiDimCreationArrSize) {
  hdf5::Dimensions DatasetDimensions{10, 10};
  {
    size_t ChunkSize{256};
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {ChunkSize});
  }
  NeXusDataset::MultiDimDataset<int> ReOpened(RootGroup,
                                              NeXusDataset::Mode::Open);
  auto DataSpace = ReOpened.dataspace();
  hdf5::dataspace::Simple SomeSpace(DataSpace);
  auto NewDims = SomeSpace.current_dimensions();
  DatasetDimensions.insert(DatasetDimensions.begin(), 0);
  EXPECT_EQ(DatasetDimensions, NewDims);
}

TEST_F(DatasetCreation, MultiDimCreationChunkSize1) {
  // Scalar chunk size
  hdf5::Dimensions DatasetDimensions{10, 10};
  size_t ChunkSize{256};
  {
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {ChunkSize});
  }
  NeXusDataset::MultiDimDataset<int> ReOpened(RootGroup,
                                              NeXusDataset::Mode::Open);
  auto CreationProperties = ReOpened.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  DatasetDimensions.insert(DatasetDimensions.begin(), ChunkSize / (10 * 10));
  EXPECT_EQ(ChunkDims, DatasetDimensions);
}

TEST_F(DatasetCreation, MultiDimCreationChunkSize2) {
  // Multi dim chunk size
  hdf5::Dimensions DatasetDimensions{10, 10};
  hdf5::Dimensions ChunkSize = {100, 20, 20};
  {
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {ChunkSize});
  }
  NeXusDataset::MultiDimDataset<int> ReOpened(RootGroup,
                                              NeXusDataset::Mode::Open);
  auto CreationProperties = ReOpened.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  EXPECT_EQ(ChunkDims, ChunkSize);
}

TEST_F(DatasetCreation, MultiDimCreationChunkSize3) {
  // Empty chunk size
  hdf5::Dimensions DatasetDimensions{10, 10};
  hdf5::Dimensions ChunkSize = {};
  {
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {ChunkSize});
  }
  NeXusDataset::MultiDimDataset<int> ReOpened(RootGroup,
                                              NeXusDataset::Mode::Open);
  auto CreationProperties = ReOpened.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  DatasetDimensions.insert(DatasetDimensions.begin(), 10);
  EXPECT_EQ(ChunkDims, DatasetDimensions);
}

TEST_F(DatasetCreation, MultiDimCreationChunkSize4) {
  // Invalid chunk size
  hdf5::Dimensions DatasetDimensions{10, 10};
  hdf5::Dimensions ChunkSize = {1, 2, 3, 4, 5};
  {
    NeXusDataset::MultiDimDataset<int> ADValues(
        RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {ChunkSize});
  }
  NeXusDataset::MultiDimDataset<int> ReOpened(RootGroup,
                                              NeXusDataset::Mode::Open);
  auto CreationProperties = ReOpened.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  DatasetDimensions.insert(DatasetDimensions.begin(), 1024);
  EXPECT_EQ(ChunkDims, DatasetDimensions);
}

TEST_F(DatasetCreation, MultiDimAppendSameSize) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  NeXusDataset::MultiDimDataset<int> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{2, 4, 6, 8};
  Dataset.appendArray(TestData, DatasetDimensions);
  DatasetDimensions.insert(DatasetDimensions.begin(), 1);
  EXPECT_EQ(DatasetDimensions, Dataset.get_extent());
  std::vector<int> StoredData(TestData.size());
  Dataset.read(StoredData);
  EXPECT_EQ(TestData, StoredData);
}

TEST_F(DatasetCreation, MultiDimAppendAnotherType) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  NeXusDataset::MultiDimDataset<double> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{2, 4, 6, 8};
  std::vector<double> CompareData{2, 4, 6, 8};
  Dataset.appendArray(TestData, DatasetDimensions);
  std::vector<double> StoredData(TestData.size());
  Dataset.read(StoredData);
  EXPECT_EQ(CompareData, StoredData);
}

TEST_F(DatasetCreation, MultiDimAppendSmallerSize1) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  auto AppendDims = hdf5::Dimensions{1, 2};
  NeXusDataset::MultiDimDataset<int> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{6, 8};
  Dataset.appendArray(TestData, AppendDims);
  DatasetDimensions.insert(DatasetDimensions.begin(), 1);
  EXPECT_EQ(DatasetDimensions, Dataset.get_extent());
  TestData = std::vector<int>{6, 8, 0, 0};
  std::vector<int> StoredData(TestData.size());
  Dataset.read(StoredData);
  EXPECT_EQ(TestData, StoredData);
}

TEST_F(DatasetCreation, MultiDimAppendWrongRank) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  NeXusDataset::MultiDimDataset<int> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{6, 8};
  EXPECT_THROW(Dataset.appendArray(TestData, {2}), std::runtime_error);
}

TEST_F(DatasetCreation, MultiDimAppendSmallerSize2) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  auto AppendDims = hdf5::Dimensions{2, 1};
  NeXusDataset::MultiDimDataset<int> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{6, 8};
  Dataset.appendArray(TestData, AppendDims);
  DatasetDimensions.insert(DatasetDimensions.begin(), 1);
  EXPECT_EQ(DatasetDimensions, Dataset.get_extent());
  TestData = std::vector<int>{6, 0, 8, 0};
  std::vector<int> StoredData(TestData.size());
  Dataset.read(StoredData);
  EXPECT_EQ(TestData, StoredData);
}

TEST_F(DatasetCreation, MultiDimAppendBiggerSize) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  auto AppendDims = hdf5::Dimensions{2, 3};
  NeXusDataset::MultiDimDataset<int> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{6, 8, 10, 12, 14, 16};
  Dataset.appendArray(TestData, AppendDims);
  DatasetDimensions.insert(DatasetDimensions.begin(), 1);
  DatasetDimensions[2] = 3;
  EXPECT_EQ(DatasetDimensions, Dataset.get_extent());
  std::vector<int> StoredData(TestData.size());
  Dataset.read(StoredData);
  EXPECT_EQ(TestData, StoredData);
}

TEST_F(DatasetCreation, MultiDimAppendWrongDimensions) {
  hdf5::Dimensions DatasetDimensions{2, 2};
  NeXusDataset::MultiDimDataset<int> Dataset(
      RootGroup, NeXusDataset::Mode::Create, DatasetDimensions, {});
  std::vector<int> TestData{6, 8};
  EXPECT_THROW(Dataset.appendArray(TestData, {2}), std::runtime_error);
}

TEST_F(DatasetCreation, AppendDataOnce) {
  int ChunkSize = 256;
  std::array<const std::uint16_t, 4> SomeData{{0, 1, 2, 3}};
  NeXusDataset::ExtensibleDataset<std::uint16_t> TestDataset(
      RootGroup, "SomeDataset", NeXusDataset::Mode::Create, ChunkSize);
  TestDataset.appendArray(SomeData);
  auto DataspaceSize = TestDataset.dataspace().size();
  EXPECT_EQ(static_cast<uint64_t>(DataspaceSize), SomeData.size());
  std::vector<std::uint16_t> Buffer(DataspaceSize);
  TestDataset.read(Buffer);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(Buffer.at(i), SomeData.at(i));
  }
}

TEST_F(DatasetCreation, AppendDataTwice) {
  int ChunkSize = 256;
  std::array<const std::uint16_t, 4> SomeData{{0, 1, 2, 3}};
  NeXusDataset::ExtensibleDataset<std::uint16_t> TestDataset(
      RootGroup, "SomeDataset", NeXusDataset::Mode::Create, ChunkSize);
  TestDataset.appendArray(SomeData);
  TestDataset.appendArray(SomeData);
  auto DataspaceSize = TestDataset.dataspace().size();
  EXPECT_EQ(static_cast<uint64_t>(DataspaceSize), SomeData.size() * 2);
  std::vector<std::uint16_t> Buffer(DataspaceSize);
  TestDataset.read(Buffer);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(Buffer.at(i), SomeData.at(i % SomeData.size()))
        << "Failed at i = " << i;
  }
}

TEST_F(DatasetCreation, AppendArrayAdpaterDataTwice) {
  int ChunkSize = 256;
  std::array<const std::uint16_t, 4> SomeData{{0, 1, 2, 3}};
  NeXusDataset::ExtensibleDataset<std::uint16_t> TestDataset(
      RootGroup, "SomeDataset", NeXusDataset::Mode::Create, ChunkSize);
  hdf5::ArrayAdapter<const std::uint16_t> TempAdapter{
      SomeData.data(), static_cast<size_t>(SomeData.size())};
  TestDataset.appendArray(TempAdapter);
  TestDataset.appendArray(TempAdapter);
  auto DataspaceSize = TestDataset.dataspace().size();
  EXPECT_EQ(static_cast<uint64_t>(DataspaceSize), SomeData.size() * 2);
  std::vector<std::uint16_t> Buffer(DataspaceSize);
  TestDataset.read(Buffer);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(Buffer.at(i), SomeData.at(i % SomeData.size()))
        << "Failed at i = " << i;
  }
}

TEST_F(DatasetCreation, StringDatasetDefaultCreation) {
  std::string DatasetName{"SomeName"};
  size_t StringLength{24};
  size_t ChunkSize{511};
  {
    NeXusDataset::FixedSizeString Strings(RootGroup, DatasetName,
                                          NeXusDataset::Mode::Create,
                                          StringLength, ChunkSize);
  }

  ASSERT_TRUE(RootGroup.has_dataset(DatasetName));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset(DatasetName);
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  hdf5::datatype::String StringType(
      hdf5::datatype::String::fixed(StringLength));
  StringType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  StringType.padding(hdf5::datatype::StringPad::NULLTERM);
  EXPECT_EQ(StringType, TestDataset.datatype());
}

TEST_F(DatasetCreation, StringDatasetReopen) {
  std::string DatasetName{"SomeName"};
  size_t StringLength{24};
  size_t ChunkSize{511};
  NeXusDataset::FixedSizeString(RootGroup, DatasetName,
                                NeXusDataset::Mode::Create, StringLength,
                                ChunkSize);

  NeXusDataset::FixedSizeString TestDataset(RootGroup, DatasetName,
                                            NeXusDataset::Mode::Open);
  EXPECT_EQ(StringLength, TestDataset.getMaxStringSize());
  EXPECT_EQ(TestDataset.dataspace().size(), 0);
}

TEST_F(DatasetCreation, StringDatasetFailReopen) {
  std::string DatasetName{"SomeName"};
  EXPECT_THROW(NeXusDataset::FixedSizeString(RootGroup, DatasetName,
                                             NeXusDataset::Mode::Open),
               std::runtime_error);
}

TEST_F(DatasetCreation, StringDatasetWriteString) {
  std::string DatasetName{"SomeName"};
  size_t StringLength{10};
  NeXusDataset::FixedSizeString TestDataset(
      RootGroup, DatasetName, NeXusDataset::Mode::Create, StringLength);

  std::string TestString{"Hello"};
  TestDataset.appendStringElement(TestString);
  std::string ReadBackString;
  TestDataset.read(ReadBackString, TestDataset.datatype(),
                   hdf5::dataspace::Scalar(),
                   hdf5::dataspace::Hyperslab{{0}, {1}});
  std::string CompareString(
      ReadBackString.data()); // Trim null characters from end of string
  EXPECT_EQ(TestString, CompareString);
}

TEST_F(DatasetCreation, StringDatasetWriteTwoStrings) {
  std::string DatasetName{"SomeName"};
  size_t StringLength{10};
  NeXusDataset::FixedSizeString TestDataset(
      RootGroup, DatasetName, NeXusDataset::Mode::Create, StringLength);

  std::string TestString1{"Hello"};
  TestDataset.appendStringElement(TestString1);
  std::string TestString2{"Hi"};
  TestDataset.appendStringElement(TestString2);

  std::string ReadBackString;
  TestDataset.read(ReadBackString, TestDataset.datatype(),
                   hdf5::dataspace::Scalar(),
                   hdf5::dataspace::Hyperslab{{1}, {1}});
  std::string CompareString(ReadBackString.data());
  EXPECT_EQ(TestString2, CompareString);
}

TEST_F(DatasetCreation, StringDatasetWriteTooLongString) {
  std::string DatasetName{"SomeName"};
  size_t StringLength{10};
  NeXusDataset::FixedSizeString TestDataset(
      RootGroup, DatasetName, NeXusDataset::Mode::Create, StringLength);

  std::string TestString{"The quick brown fox jumped over the lazy turtle"};
  TestDataset.appendStringElement(TestString);
  std::string ReadBackString;
  TestDataset.read(ReadBackString, TestDataset.datatype(),
                   hdf5::dataspace::Scalar(),
                   hdf5::dataspace::Hyperslab{{0}, {1}});
  std::string CompareString(ReadBackString.data());
  EXPECT_NE(TestString, CompareString);
  EXPECT_EQ(std::string(TestString.begin(), TestString.begin() + StringLength),
            CompareString);
}
