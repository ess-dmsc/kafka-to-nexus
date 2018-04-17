#include <gtest/gtest.h>
#include "schemas/senv/Datasets.h"

class DatasetCreation : public ::testing::Test {
public:
  void SetUp() override {
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE);
    RootGroup = File.root();
  };
  
  void TearDown() override {
    File.close();
  };
  std::string TestFileName{"DatasetCreationTestFile.hdf5"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
};

TEST_F(DatasetCreation, AppendDataOnce) {
  int ChunkSize = 256;
  std::array<const std::uint16_t, 4> SomeData{{0, 1, 2, 3}};
  NeXusDataset::ExtensibleDataset<std::uint16_t> TestDataset(RootGroup, "SomeDataset", ChunkSize);
  TestDataset.appendData(SomeData);
  auto DataspaceSize = TestDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, SomeData.size());
  std::vector<std::uint16_t> Buffer(DataspaceSize);
  TestDataset.read(Buffer);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(Buffer.at(i), SomeData.at(i));
  }
}

TEST_F(DatasetCreation, AppendDataTwice) {
  int ChunkSize = 256;
  std::array<const std::uint16_t, 4> SomeData{{0, 1, 2, 3}};
  NeXusDataset::ExtensibleDataset<std::uint16_t> TestDataset(RootGroup, "SomeDataset", ChunkSize);
  TestDataset.appendData(SomeData);
  TestDataset.appendData(SomeData);
  auto DataspaceSize = TestDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, SomeData.size() * 2);
  std::vector<std::uint16_t> Buffer(DataspaceSize);
  TestDataset.read(Buffer);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(Buffer.at(i), SomeData.at(i % SomeData.size())) << "Failed at i = " << i;
  }
}

//--------------------------------------------------

TEST_F(DatasetCreation, RawValueDefaultCreation) {
  int ChunkSize = 256;
  {
    NeXusDataset::RawValue ADCValues(RootGroup, ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("raw_value"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("raw_value");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint16_t>(), TestDataset.datatype());
}

TEST_F(DatasetCreation, RawValueReOpen) {
  int ChunkSize = 256;
  {
  NeXusDataset::RawValue ADCValues(RootGroup, ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::RawValue ReOpened(RootGroup));
}

TEST_F(DatasetCreation, RawValueThrowOnExists) {
  int ChunkSize = 256;
  {
  NeXusDataset::RawValue ADCValues(RootGroup, ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::RawValue ADCValues(RootGroup, ChunkSize), std::runtime_error);
}

//--------------------------------------------------

TEST_F(DatasetCreation, TimeDefaultCreation) {
  int ChunkSize = 256;
  {
  NeXusDataset::Time Timestamps(RootGroup, ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("time"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("time");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint64_t>(), TestDataset.datatype());
}

TEST_F(DatasetCreation, TimeReOpen) {
  int ChunkSize = 256;
  {
  NeXusDataset::Time Timestamps(RootGroup, ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::Time ReOpened(RootGroup));
}

TEST_F(DatasetCreation, TimeThrowOnExists) {
  int ChunkSize = 256;
  {
  NeXusDataset::Time Timestamps(RootGroup, ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::Time Timestamps(RootGroup, ChunkSize), std::runtime_error);
}

//--------------------------------------------------

TEST_F(DatasetCreation, CueIndexDefaultCreation) {
  int ChunkSize = 256;
  {
  NeXusDataset::CueIndex Cue(RootGroup, ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("cue_index"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("cue_index");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint32_t>(), TestDataset.datatype());
}

TEST_F(DatasetCreation, CueIndexReOpen) {
  int ChunkSize = 256;
  {
  NeXusDataset::CueIndex Cue(RootGroup, ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::CueIndex ReOpened(RootGroup));
}

TEST_F(DatasetCreation, CueIndexThrowOnExists) {
  int ChunkSize = 256;
  {
  NeXusDataset::CueIndex Cue(RootGroup, ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::CueIndex Cue(RootGroup, ChunkSize), std::runtime_error);
}

//--------------------------------------------------

TEST_F(DatasetCreation, CueTimestampZeroDefaultCreation) {
  int ChunkSize = 256;
  {
  NeXusDataset::CueTimestampZero Cue(RootGroup, ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("cue_timestamp_zero"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("cue_timestamp_zero");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint64_t>(), TestDataset.datatype());
}

TEST_F(DatasetCreation, CueTimestampZeroReOpen) {
  int ChunkSize = 256;
  {
  NeXusDataset::CueTimestampZero Cue(RootGroup, ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::CueTimestampZero ReOpened(RootGroup));
}

TEST_F(DatasetCreation, CueTimestampZeroThrowOnExists) {
  int ChunkSize = 256;
  {
  NeXusDataset::CueTimestampZero Cue(RootGroup, ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::CueTimestampZero Cue(RootGroup, ChunkSize), std::runtime_error);
}
