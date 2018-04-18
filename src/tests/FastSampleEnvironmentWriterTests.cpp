#include "schemas/senv/FastSampleEnvironmentWriter.h"
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

class FastSampleEnvironmentReader : public ::testing::Test {
public:
  static void SetUpTestCase() {
    flatbuffers::FlatBufferBuilder builder;
    std::vector<std::uint16_t> TestValues{0, 1, 2, 3, 4, 5};
    std::vector<std::uint64_t> TestTimestamps{1, 2, 3, 4, 5, 6};
    auto FBValuesOffset = builder.CreateVector(TestValues);
    auto FBTimestampOffset = builder.CreateVector(TestTimestamps);
    auto FBNameStringOffset = builder.CreateString("SomeTestString");
    SampleEnvironmentDataBuilder MessageBuilder(builder);
    MessageBuilder.add_Name(FBNameStringOffset);
    MessageBuilder.add_Values(FBValuesOffset);
    MessageBuilder.add_Timestamps(FBTimestampOffset);
    MessageBuilder.add_Channel(42);
    MessageBuilder.add_PacketTimestamp(123456789);
    MessageBuilder.add_TimeDelta(0.565656);
    MessageBuilder.add_MessageCounter(987654321);
    MessageBuilder.add_TimestampLocation(Location::Middle);
    builder.Finish(MessageBuilder.Finish(), SampleEnvironmentDataIdentifier());
    FileSize = builder.GetSize();
    RawBuffer.reset(new std::int8_t[FileSize]);
    std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), FileSize);
  };

  void SetUp() override {
    ASSERT_NE(RawBuffer.get(), nullptr);
    ReaderUnderTest.reset(new senv::SampleEnvironmentDataGuard());
  };

  std::unique_ptr<senv::SampleEnvironmentDataGuard> ReaderUnderTest;
  static std::unique_ptr<std::int8_t[]> RawBuffer;
  static size_t FileSize;
};
std::unique_ptr<std::int8_t[]> FastSampleEnvironmentReader::RawBuffer{nullptr};
size_t FastSampleEnvironmentReader::FileSize{0};

TEST_F(FastSampleEnvironmentReader, GetSourceName) {
  FileWriter::Msg TestMessage =
      FileWriter::Msg::owned((char *)RawBuffer.get(), FileSize);
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), "SomeTestString");
}

TEST_F(FastSampleEnvironmentReader, GetTimeStamp) {
  FileWriter::Msg TestMessage =
      FileWriter::Msg::owned((char *)RawBuffer.get(), FileSize);
  EXPECT_EQ(ReaderUnderTest->timestamp(TestMessage), 123456789);
}

TEST_F(FastSampleEnvironmentReader, Verify) {
  FileWriter::Msg TestMessage =
      FileWriter::Msg::owned((char *)RawBuffer.get(), FileSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}

TEST_F(FastSampleEnvironmentReader, VerifyFail) {
  std::unique_ptr<char[]> TempData(new char[FileSize]);
  std::memcpy(TempData.get(), RawBuffer.get(), FileSize);
  FileWriter::Msg TestMessage1 =
      FileWriter::Msg::owned(TempData.get(), FileSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage1));
  TempData[4] = 'h';
  FileWriter::Msg TestMessage2 =
      FileWriter::Msg::owned(TempData.get(), FileSize);
  EXPECT_FALSE(ReaderUnderTest->verify(TestMessage2));
}

class FastSampleEnvironmentWriter : public ::testing::Test {
public:
  void SetUp() override {
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE);
    RootGroup = File.root();
    UsedGroup = RootGroup.create_group(NXLogGroup);
  };

  void TearDown() override { File.close(); };
  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

TEST_F(FastSampleEnvironmentWriter, InitFile) {
  {
    senv::FastSampleEnvironmentWriter Writer;
    EXPECT_TRUE(
        Writer.init_hdf(UsedGroup, nullptr).is_OK());
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("raw_value"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("time"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(FastSampleEnvironmentWriter, ReopenFileFailure) {
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup).is_OK());
}

TEST_F(FastSampleEnvironmentWriter, ReopenFileSuccess) {
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, nullptr).is_OK());
  EXPECT_TRUE(Writer.reopen(UsedGroup).is_OK());
}
