// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "NeXusDataset/NeXusDataset.h"
#include <gtest/gtest.h>
#include <h5cpp/datatype/type_trait.hpp>
#include <h5cpp/hdf5.hpp>

class NeXusDatasetCreation : public ::testing::Test {
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

template <class Dataset, typename DatasetType>
void defaultDatasetCreation(hdf5::node::Group &RootGroup,
                            std::string DatasetName) {
  size_t ChunkSize = 256;
  { Dataset UnderTest(RootGroup, NeXusDataset::Mode::Create, ChunkSize); }
  ASSERT_TRUE(RootGroup.has_dataset(DatasetName))
      << "Missing dataset: " << DatasetName;
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset(DatasetName);
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<DatasetType>(), TestDataset.datatype())
      << "Wrong type for dataset: " << DatasetName;
  RootGroup.remove(DatasetName);
}

template <class Dataset, typename DatasetType>
void defaultTimeDatasetCreation(hdf5::node::Group &RootGroup,
                                std::string DatasetName) {
  size_t ChunkSize = 256;
  { Dataset UnderTest(RootGroup, NeXusDataset::Mode::Create, ChunkSize); }
  ASSERT_TRUE(RootGroup.has_dataset(DatasetName))
      << "Missing dataset: " << DatasetName;
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset(DatasetName);
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<DatasetType>(), TestDataset.datatype())
      << "Wrong type for dataset: " << DatasetName;
  bool FoundStartAttr{false};
  bool FoundUnitAttr{false};
  for (const auto &Attribute : TestDataset.attributes) {
    std::string AttributeValue;
    if (Attribute.name() == "start") {
      Attribute.read(AttributeValue);
      if (AttributeValue == "1970-01-01T00:00:00Z") {
        FoundStartAttr = true;
      }
    } else if (Attribute.name() == "units") {
      Attribute.read(AttributeValue);
      if (AttributeValue == "ns") {
        FoundUnitAttr = true;
      }
    }
  }
  EXPECT_TRUE(FoundStartAttr);
  EXPECT_TRUE(FoundUnitAttr);
  RootGroup.remove(DatasetName);
}

template <class Dataset>
void reOpenDataset(hdf5::node::Group &RootGroup, std::string DatasetName) {
  {
    size_t ChunkSize{256};
    Dataset UnderTest(RootGroup, NeXusDataset::Mode::Create, ChunkSize);
  }
  EXPECT_NO_THROW(Dataset ReOpened(RootGroup, NeXusDataset::Mode::Open))
      << "Unable to re-open dataset.";
  RootGroup.remove(DatasetName);
}

template <class Dataset>
void failOnReCreateDataset(hdf5::node::Group &RootGroup,
                           std::string DatasetName) {
  size_t ChunkSize = 256;
  { Dataset UnderTest(RootGroup, NeXusDataset::Mode::Create, ChunkSize); }
  EXPECT_THROW(
      Dataset UnderTest(RootGroup, NeXusDataset::Mode::Create, ChunkSize),
      std::runtime_error)
      << "Re-creation of dataset should fail but did not.";
  RootGroup.remove(DatasetName);
}

template <class Dataset> void wrongModeOpen(hdf5::node::Group &RootGroup) {
  size_t ChunkSize = 256;
  EXPECT_THROW(Dataset(RootGroup, NeXusDataset::Mode(-1247832), ChunkSize),
               std::runtime_error)
      << "Should have failed to open (but did not) due to wrong mode.";
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, RawValueOpen) {
  using TypeUnderTest = NeXusDataset::UInt16Value;
  std::string DatasetName{"value"};
  defaultDatasetCreation<TypeUnderTest, std::uint16_t>(RootGroup, DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, TimeOpen) {
  using TypeUnderTest = NeXusDataset::Time;
  std::string DatasetName{"time"};
  defaultTimeDatasetCreation<TypeUnderTest, std::uint64_t>(RootGroup,
                                                           DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, CueIndexOpen) {
  using TypeUnderTest = NeXusDataset::CueIndex;
  std::string DatasetName{"cue_index"};
  defaultDatasetCreation<TypeUnderTest, std::uint32_t>(RootGroup, DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, CueTimestampZeroOpen) {
  using TypeUnderTest = NeXusDataset::CueTimestampZero;
  std::string DatasetName{"cue_timestamp_zero"};
  defaultTimeDatasetCreation<TypeUnderTest, std::uint64_t>(RootGroup,
                                                           DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventIdOpen) {
  using TypeUnderTest = NeXusDataset::EventId;
  std::string DatasetName{"event_id"};
  defaultDatasetCreation<TypeUnderTest, std::uint32_t>(RootGroup, DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventIndexOpen) {
  using TypeUnderTest = NeXusDataset::EventIndex;
  std::string DatasetName{"event_index"};
  defaultDatasetCreation<TypeUnderTest, std::uint32_t>(RootGroup, DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventTimeOffsetOpen) {
  using TypeUnderTest = NeXusDataset::EventTimeOffset;
  std::string DatasetName{"event_time_offset"};
  defaultDatasetCreation<TypeUnderTest, std::uint32_t>(RootGroup, DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventTimeZeroOpen) {
  using TypeUnderTest = NeXusDataset::EventTimeZero;
  std::string DatasetName{"event_time_zero"};
  defaultTimeDatasetCreation<TypeUnderTest, std::uint64_t>(RootGroup,
                                                           DatasetName);
  reOpenDataset<TypeUnderTest>(RootGroup, DatasetName);
  wrongModeOpen<TypeUnderTest>(RootGroup);
  failOnReCreateDataset<TypeUnderTest>(RootGroup, DatasetName);
}
