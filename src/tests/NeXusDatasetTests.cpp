// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "NeXusDataset.h"
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

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, RawValueDefaultCreation) {
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

TEST_F(NeXusDatasetCreation, RawValueConstructorFail) {
  size_t ChunkSize = 256;
  EXPECT_THROW(NeXusDataset::RawValue(RootGroup, NeXusDataset::Mode(-1247832),
                                      ChunkSize),
               std::runtime_error);
}

TEST_F(NeXusDatasetCreation, RawValueReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::RawValue ADCValues(RootGroup, NeXusDataset::Mode::Create,
                                     ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::RawValue ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, RawValueThrowOnExists) {
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

TEST_F(NeXusDatasetCreation, TimeDefaultCreation) {
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
}

TEST_F(NeXusDatasetCreation, TimeReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::Time Timestamps(RootGroup, NeXusDataset::Mode::Create,
                                  ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::Time ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, TimeThrowOnExists) {
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

TEST_F(NeXusDatasetCreation, CueIndexDefaultCreation) {
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

TEST_F(NeXusDatasetCreation, CueIndexReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::CueIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                               ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::CueIndex ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, CueIndexThrowOnExists) {
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

TEST_F(NeXusDatasetCreation, CueTimestampZeroDefaultCreation) {
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
}

TEST_F(NeXusDatasetCreation, CueTimestampZeroReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::CueTimestampZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                       ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::CueTimestampZero ReOpened(
      RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, CueTimestampZeroThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::CueTimestampZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                       ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::CueTimestampZero Cue(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventIdDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventId Cue(RootGroup, NeXusDataset::Mode::Create, ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("event_id"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("event_id");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint32_t>(), TestDataset.datatype());
}

TEST_F(NeXusDatasetCreation, EventIdReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::EventId Cue(RootGroup, NeXusDataset::Mode::Create, ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::EventId ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, EventIdThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventId Cue(RootGroup, NeXusDataset::Mode::Create, ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::EventId Cue(RootGroup, NeXusDataset::Mode::Create,
                                         ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventIndexDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                                 ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("event_index"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("event_index");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint32_t>(), TestDataset.datatype());
}

TEST_F(NeXusDatasetCreation, EventIndexReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::EventIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                                 ChunkSize);
  }
  EXPECT_NO_THROW(
      NeXusDataset::EventIndex ReOpened(RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, EventIndexThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventIndex Cue(RootGroup, NeXusDataset::Mode::Create,
                                 ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::EventIndex Cue(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventTimeOffsetDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventTimeOffset Cue(RootGroup, NeXusDataset::Mode::Create,
                                      ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("event_time_offset"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("event_time_offset");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint32_t>(), TestDataset.datatype());
}

TEST_F(NeXusDatasetCreation, EventTimeOffsetReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::EventTimeOffset Cue(RootGroup, NeXusDataset::Mode::Create,
                                      ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::EventTimeOffset ReOpened(
      RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, EventTimeOffsetThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventTimeOffset Cue(RootGroup, NeXusDataset::Mode::Create,
                                      ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::EventTimeOffset Cue(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}

//--------------------------------------------------

TEST_F(NeXusDatasetCreation, EventTimeZeroDefaultCreation) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventTimeZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                    ChunkSize);
  }
  ASSERT_TRUE(RootGroup.has_dataset("event_time_zero"));
  hdf5::node::Dataset TestDataset = RootGroup.get_dataset("event_time_zero");
  auto CreationProperties = TestDataset.creation_list();
  auto ChunkDims = CreationProperties.chunk();
  ASSERT_EQ(ChunkDims.size(), 1u);
  EXPECT_EQ(ChunkDims.at(0), ChunkSize);
  EXPECT_EQ(hdf5::datatype::create<std::uint64_t>(), TestDataset.datatype());
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
}

TEST_F(NeXusDatasetCreation, EventTimeZeroReOpen) {
  {
    size_t ChunkSize{256};
    NeXusDataset::EventTimeZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                    ChunkSize);
  }
  EXPECT_NO_THROW(NeXusDataset::EventTimeZero ReOpened(
      RootGroup, NeXusDataset::Mode::Open));
}

TEST_F(NeXusDatasetCreation, EventTimeZeroThrowOnExists) {
  size_t ChunkSize = 256;
  {
    NeXusDataset::EventTimeZero Cue(RootGroup, NeXusDataset::Mode::Create,
                                    ChunkSize);
  }
  EXPECT_THROW(NeXusDataset::EventTimeZero Cue(
                   RootGroup, NeXusDataset::Mode::Create, ChunkSize),
               std::runtime_error);
}
