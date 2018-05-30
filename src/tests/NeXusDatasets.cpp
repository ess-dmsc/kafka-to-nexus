#include "schemas/senv/Datasets.h"
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

//--------------------------------------------------

TEST_F(DatasetCreation, RawValueDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::RawValue ADCValues(RootGroup, NeXusDataset::Mode::Create,
                                     ChunkSize);
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
  size_t ChunkSize = 256;
  {
    NeXusDataset::RawValue ADCValues(RootGroup, NeXusDataset::Mode::Create,
                                     ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::RawValue ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(DatasetCreation, RawValueThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::RawValue ADCValues(RootGroup, NeXusDataset::Mode::Create,
                                     ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::RawValue ADCValues(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(DatasetCreation, TimeDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::Time Timestamps(RootGroup, NeXusDataset::Mode::Create,
                                  ChunkSize);
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
  size_t ChunkSize = 256;
  {
    NeXusDataset::Time Timestamps(RootGroup, NeXusDataset::Mode::Create,
                                  ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::Time ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(DatasetCreation, TimeThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::Time Timestamps(RootGroup, NeXusDataset::Mode::Create,
                                  ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::Time Timestamps(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(DatasetCreation, CueIndexDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                               ChunkSize);
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
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                               ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::CueIndex ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(DatasetCreation, CueIndexThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                               ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::CueIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                                          ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(DatasetCreation, CueTimestampZeroDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueTimestampZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                       ChunkSize);
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
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueTimestampZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                       ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::CueTimestampZero ReOpened(
      RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(DatasetCreation, CueTimestampZeroThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueTimestampZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                       ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::CueTimestampZero Cue(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}
