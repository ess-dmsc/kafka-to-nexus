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
#include <tdct_timestamps_generated.h>

#include "AccessMessageMetadata/tdct/tdct_Extractor.h"
#include "WriterModule/tdct/tdct_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

static std::unique_ptr<std::uint8_t[]>
GenerateFlatbufferData(size_t &DataSize) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::uint64_t> TestTimestamps{11, 22, 33, 44, 55, 66};
  auto FBTimestampOffset = builder.CreateVector(TestTimestamps);
  auto FBNameStringOffset = builder.CreateString("SomeTestString");
  timestampBuilder MessageBuilder(builder);
  MessageBuilder.add_name(FBNameStringOffset);
  MessageBuilder.add_timestamps(FBTimestampOffset);
  builder.Finish(MessageBuilder.Finish(), timestampIdentifier());
  DataSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::uint8_t[]>(DataSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), DataSize);
  return RawBuffer;
}

using FBMsg = FileWriter::FlatbufferMessage;
using namespace WriterModule;

class ChopperTimeStampWriter : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(NXLogGroup);
    setExtractorModule<AccessMessageMetadata::tdct_Extractor>("tdct");
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

using WriterModule::InitResult;

TEST_F(ChopperTimeStampWriter, InitFile) {
  {
    tdct::tdct_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("time"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(ChopperTimeStampWriter, ReopenFileFailure) {
  tdct::tdct_Writer Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(ChopperTimeStampWriter, InitFileFail) {
  tdct::tdct_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup) == InitResult::OK);
}

TEST_F(ChopperTimeStampWriter, ReopenFileSuccess) {
  tdct::tdct_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(ChopperTimeStampWriter, WriteDataOnce) {
  size_t BufferSize;
  auto Buffer = GenerateFlatbufferData(BufferSize);
  tdct::tdct_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  auto FbPointer = Gettimestamp(TestMsg.data());

  auto DataspaceSize = TimestampDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, FbPointer->timestamps()->size());
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  TimestampDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), FbPointer->timestamps()->operator[](i));
  }

  std::vector<std::int32_t> CueIndex(1);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0);

  std::vector<std::uint32_t> CueTimestamp(1);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->timestamps()->operator[](0));
}

TEST_F(ChopperTimeStampWriter, WriteDataTwice) {
  size_t BufferSize;
  auto Buffer = GenerateFlatbufferData(BufferSize);
  tdct::tdct_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  auto FbPointer = Gettimestamp(TestMsg.data());

  auto DataspaceSize = TimestampDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, FbPointer->timestamps()->size() * 2);
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  TimestampDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), FbPointer->timestamps()->operator[](
                                        i % FbPointer->timestamps()->size()));
  }

  std::vector<std::uint32_t> CueIndex(2);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0u);
  EXPECT_EQ(CueIndex.at(1), FbPointer->timestamps()->size());

  std::vector<std::uint32_t> CueTimestamp(2);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->timestamps()->operator[](0));
  EXPECT_EQ(CueTimestamp.at(1), FbPointer->timestamps()->operator[](0));
}

TEST_F(ChopperTimeStampWriter, WriteNoElements) {
  size_t BufferSize;
  auto Buffer = GenerateFlatbufferData(BufferSize);
  auto FbPointer = Gettimestamp(Buffer.get());
  auto ValueLengthPtr =
      reinterpret_cast<flatbuffers::uoffset_t *>(
          const_cast<std::uint8_t *>(FbPointer->timestamps()->Data())) -
      1;
  *ValueLengthPtr = 0;
  tdct::tdct_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  EXPECT_THROW(FileWriter::FlatbufferMessage(Buffer.get(), BufferSize),
               std::runtime_error);
}
