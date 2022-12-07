// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include <memory>
#include <se00_data_generated.h>

#include "AccessMessageMetadata/se00/se00_Extractor.h"
#include "WriterModule/se00/se00_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

namespace se00_tests {
  std::unique_ptr<std::uint8_t[]>
  GenerateFlatbufferData(size_t &DataSize, size_t NrOfElements = 6,
                         bool CreateTimestamps = true) {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<std::uint16_t> TestValues(NrOfElements);
    std::vector<std::int64_t> TestTimestamps(NrOfElements);
    for (size_t i = 0; i < NrOfElements; i++) {
      float value = (i + 1) / 10.;
      TestValues.push_back(value);
      TestTimestamps.push_back(i);
    }
    
    auto FBValuesOffset = builder.CreateVector(TestValues);
    auto ValueObjectOffset = CreateUInt16Array(builder, FBValuesOffset);
    auto FBTimestampOffset = builder.CreateVector(TestTimestamps);
    auto FBNameStringOffset = builder.CreateString("SomeTestString");
    se00_SampleEnvironmentDataBuilder MessageBuilder(builder);
    MessageBuilder.add_name(FBNameStringOffset);
    MessageBuilder.add_values(ValueObjectOffset.Union());
    MessageBuilder.add_values_type(ValueUnion::UInt16Array);
    if (CreateTimestamps) {
      MessageBuilder.add_timestamps(FBTimestampOffset);
    }
    MessageBuilder.add_channel(42);
    MessageBuilder.add_packet_timestamp(123456789);
    MessageBuilder.add_time_delta(0.565656);
    MessageBuilder.add_message_counter(987654321);
    MessageBuilder.add_timestamp_location(Location::Middle);
    builder.Finish(MessageBuilder.Finish(), se00_SampleEnvironmentDataIdentifier());
    DataSize = builder.GetSize();
    auto RawBuffer = std::make_unique<std::uint8_t[]>(DataSize);
    std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), DataSize);
    return RawBuffer;
  }
}

class se00Writer : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(NXLogGroup);
    setExtractorModule<AccessMessageMetadata::se00_Extractor>("se00");
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

using WriterModule::InitResult;

TEST_F(se00Writer, InitFile) {
  {
    WriterModule::se00::se00_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("value"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("time"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(se00Writer, ReopenFileFailure) {
  WriterModule::se00::se00_Writer Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(se00Writer, InitFileFail) {
  WriterModule::se00::se00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup) == InitResult::OK);
}

TEST_F(se00Writer, ReopenFileSuccess) {
  WriterModule::se00::se00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(se00Writer, WriteDataOnce) {
  size_t BufferSize;
  auto Buffer = se00_tests::GenerateFlatbufferData(BufferSize);
  WriterModule::se00::se00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto RawValuesDataset = UsedGroup.get_dataset("value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  auto FbPointer = Getse00_SampleEnvironmentData(TestMsg.data());

  auto ValuesObjPtr = FbPointer->values_as_UInt16Array()->value();
  auto ValuesSize = ValuesObjPtr->size();

  auto DataspaceSize = RawValuesDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, ValuesSize);
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  RawValuesDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), ValuesObjPtr->operator[](i));
  }

  auto NrOfTimeStampElements = TimestampDataset.dataspace().size();
  EXPECT_EQ(NrOfTimeStampElements, DataspaceSize);
  std::vector<std::int64_t> TimestampsVector(NrOfTimeStampElements);
  TimestampDataset.read(TimestampsVector);
  for (int j = 0; j < DataspaceSize; j++) {
    EXPECT_EQ(TimestampsVector.at(j), FbPointer->timestamps()->operator[](j));
  }

  std::vector<std::int32_t> CueIndex(1);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0);

  std::vector<std::uint32_t> CueTimestamp(1);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->packet_timestamp());
}

TEST_F(se00Writer, WriteDataTwice) {
  size_t BufferSize;
  auto Buffer = se00_tests::GenerateFlatbufferData(BufferSize);
  WriterModule::se00::se00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto RawValuesDataset = UsedGroup.get_dataset("value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  auto FbPointer = Getse00_SampleEnvironmentData(TestMsg.data());

  auto ValuesObjPtr = FbPointer->values_as_UInt16Array()->value();
  auto ValuesSize = ValuesObjPtr->size();

  auto DataspaceSize = RawValuesDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, ValuesSize * 2);
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  RawValuesDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), ValuesObjPtr->operator[](i % ValuesSize));
  }

  EXPECT_EQ(TimestampDataset.dataspace().size(), DataspaceSize);

  std::vector<std::uint32_t> CueIndex(2);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0u);
  EXPECT_EQ(CueIndex.at(1), ValuesSize);

  std::vector<std::uint32_t> CueTimestamp(2);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->packet_timestamp());
  EXPECT_EQ(CueTimestamp.at(1), FbPointer->packet_timestamp());
}

TEST_F(se00Writer, WriteNoElements) {
  size_t BufferSize;
  auto Buffer = se00_tests::GenerateFlatbufferData(BufferSize, 0);
  WriterModule::se00::se00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto RawValuesDataset = UsedGroup.get_dataset("value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  EXPECT_EQ(RawValuesDataset.dataspace().size(), 0);
  EXPECT_EQ(TimestampDataset.dataspace().size(), 0);
  EXPECT_EQ(CueIndexDataset.dataspace().size(), 0);
  EXPECT_EQ(CueTimestampZeroDataset.dataspace().size(), 0);
}

TEST_F(se00Writer, WriteDataWithNoTimestampsInFB) {
  size_t BufferSize;
  auto Buffer = se00_tests::GenerateFlatbufferData(BufferSize, 16, false);
  auto FbPointer = Getse00_SampleEnvironmentData(Buffer.get());
  WriterModule::se00::se00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto RawValuesDataset = UsedGroup.get_dataset("value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");

  auto ValuesObjPtr = FbPointer->values_as_UInt16Array()->value();
  auto ValuesSize = ValuesObjPtr->size();

  auto DataspaceSize = RawValuesDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, ValuesSize);
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  RawValuesDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), ValuesObjPtr->operator[](i));
  }

  auto NrOfTimeStampElements = TimestampDataset.dataspace().size();
  EXPECT_EQ(NrOfTimeStampElements, DataspaceSize);
  std::vector<std::int64_t> TimestampsVector(NrOfTimeStampElements);
  TimestampDataset.read(TimestampsVector);
  EXPECT_EQ(TimestampsVector.at(0), FbPointer->packet_timestamp());

  std::vector<std::int32_t> CueIndex(1);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0);

  std::vector<std::uint32_t> CueTimestamp(1);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->packet_timestamp());
}
