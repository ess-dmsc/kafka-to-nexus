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

using FileWriter::FlatbufferReaderRegistry::ReaderPtr;
using FBMsg = FileWriter::FlatbufferMessage;

class ChopperTimeStampGuard : public ::testing::Test {
public:
  static void SetUpTestCase() {
    ReaderUnderTest = std::make_unique<AccessMessageMetadata::tdct_Extractor>();
    setExtractorModule<AccessMessageMetadata::tdct_Extractor>("tdct");
    RawBuffer = GenerateFlatbufferData(BufferSize);
    TestMessage = std::make_unique<FBMsg>(RawBuffer.get(), BufferSize);
  };

  void SetUp() override { ASSERT_NE(RawBuffer.get(), nullptr); };

  static std::unique_ptr<AccessMessageMetadata::tdct_Extractor> ReaderUnderTest;
  static std::unique_ptr<std::uint8_t[]> RawBuffer;
  static size_t BufferSize;
  static std::unique_ptr<FBMsg> TestMessage;
};
std::unique_ptr<std::uint8_t[]> ChopperTimeStampGuard::RawBuffer{nullptr};
size_t ChopperTimeStampGuard::BufferSize{0};
std::unique_ptr<FBMsg> ChopperTimeStampGuard::TestMessage{nullptr};
std::unique_ptr<AccessMessageMetadata::tdct_Extractor>
    ChopperTimeStampGuard::ReaderUnderTest{nullptr};

TEST_F(ChopperTimeStampGuard, GetSourceName) {
  EXPECT_EQ(ReaderUnderTest->source_name(*TestMessage), "SomeTestString");
}

TEST_F(ChopperTimeStampGuard, GetTimeStamp) {
  EXPECT_EQ(ReaderUnderTest->timestamp(*TestMessage), 11u);
}

TEST_F(ChopperTimeStampGuard, Verify) {
  EXPECT_TRUE(ReaderUnderTest->verify(*TestMessage));
}

TEST_F(ChopperTimeStampGuard, VerifyFail) {
  auto TempData = std::make_unique<uint8_t[]>(BufferSize);
  std::memcpy(TempData.get(), RawBuffer.get(), BufferSize);
  FileWriter::FlatbufferMessage TestMessage1(TempData.get(), BufferSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage1));
  TempData[3] = 'h';
  EXPECT_THROW(FileWriter::FlatbufferMessage(TempData.get(), BufferSize),
               FileWriter::NotValidFlatbuffer);
}
