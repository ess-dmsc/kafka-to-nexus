// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "AccessMessageMetadata/senv/senv_Extractor.h"
#include "helpers/SetExtractorModule.h"
#include <gtest/gtest.h>
#include <memory>
#include <senv_data_generated.h>

static std::unique_ptr<std::uint8_t[]>
GenerateFlatbufferData(size_t &DataSize) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::uint16_t> TestValues{0, 1, 2, 3, 4, 5};
  std::vector<std::uint64_t> TestTimestamps{1, 2, 3, 4, 5, 6};

  auto FBValuesOffset = builder.CreateVector(TestValues);
  auto ValueObjectOffset = CreateUInt16Array(builder, FBValuesOffset);
  auto FBTimestampOffset = builder.CreateVector(TestTimestamps);
  auto FBNameStringOffset = builder.CreateString("SomeTestString");
  SampleEnvironmentDataBuilder MessageBuilder(builder);
  MessageBuilder.add_Name(FBNameStringOffset);
  MessageBuilder.add_Values(ValueObjectOffset.Union());
  MessageBuilder.add_Values_type(ValueUnion::UInt16Array);
  MessageBuilder.add_Timestamps(FBTimestampOffset);
  MessageBuilder.add_Channel(42);
  MessageBuilder.add_PacketTimestamp(123456789);
  MessageBuilder.add_TimeDelta(0.565656);
  MessageBuilder.add_MessageCounter(987654321);
  MessageBuilder.add_TimestampLocation(Location::Middle);
  builder.Finish(MessageBuilder.Finish(), SampleEnvironmentDataIdentifier());
  DataSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::uint8_t[]>(DataSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), DataSize);
  return RawBuffer;
}

using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

class FastSampleEnvironmentReader : public ::testing::Test {
public:
  static void SetUpTestCase() {
    RawBuffer = GenerateFlatbufferData(BufferSize);
  };

  void SetUp() override {
    ASSERT_NE(RawBuffer.get(), nullptr);
    ReaderUnderTest = std::make_unique<AccessMessageMetadata::senv_Extractor>();
    setExtractorModule<AccessMessageMetadata::senv_Extractor>("senv");
  };

  std::unique_ptr<AccessMessageMetadata::senv_Extractor> ReaderUnderTest;
  static std::unique_ptr<std::uint8_t[]> RawBuffer;
  static size_t BufferSize;
};
std::unique_ptr<std::uint8_t[]> FastSampleEnvironmentReader::RawBuffer{nullptr};
size_t FastSampleEnvironmentReader::BufferSize{0};

TEST_F(FastSampleEnvironmentReader, GetSourceName) {
  FileWriter::FlatbufferMessage TestMessage(RawBuffer.get(), BufferSize);
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), "SomeTestString");
}

TEST_F(FastSampleEnvironmentReader, GetTimeStamp) {
  FileWriter::FlatbufferMessage TestMessage(RawBuffer.get(), BufferSize);
  EXPECT_EQ(ReaderUnderTest->timestamp(TestMessage), 123456789u);
}

TEST_F(FastSampleEnvironmentReader, Verify) {
  FileWriter::FlatbufferMessage TestMessage(RawBuffer.get(), BufferSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}

TEST_F(FastSampleEnvironmentReader, VerifyFail) {
  auto TempData = std::make_unique<uint8_t[]>(BufferSize);
  std::memcpy(TempData.get(), RawBuffer.get(), BufferSize);
  FileWriter::FlatbufferMessage TestMessage1(TempData.get(), BufferSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage1));
  TempData[3] = 'h';
  EXPECT_THROW(FileWriter::FlatbufferMessage(TempData.get(), BufferSize),
               FileWriter::NotValidFlatbuffer);
}
