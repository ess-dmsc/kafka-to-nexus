#include "schemas/senv/FastSampleEnvironmentWriter.h"
#include "senv_data_generated.h"
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

std::unique_ptr<std::int8_t[]> GenerateFlatbufferData(size_t &DataSize) {
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
  DataSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::int8_t[]>(DataSize);
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
    ReaderUnderTest = std::make_unique<senv::SampleEnvironmentDataGuard>();
    std::map<std::string, ReaderPtr> &Readers =
        FileWriter::FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    FileWriter::FlatbufferReaderRegistry::Registrar<
        senv::SampleEnvironmentDataGuard>
        RegisterIt("senv");
  };

  std::unique_ptr<senv::SampleEnvironmentDataGuard> ReaderUnderTest;
  static std::unique_ptr<std::int8_t[]> RawBuffer;
  static size_t BufferSize;
};
std::unique_ptr<std::int8_t[]> FastSampleEnvironmentReader::RawBuffer{nullptr};
size_t FastSampleEnvironmentReader::BufferSize{0};

TEST_F(FastSampleEnvironmentReader, GetSourceName) {
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(RawBuffer.get()), BufferSize);
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), "SomeTestString");
}

TEST_F(FastSampleEnvironmentReader, GetTimeStamp) {
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(RawBuffer.get()), BufferSize);
  EXPECT_EQ(ReaderUnderTest->timestamp(TestMessage), 123456789u);
}

TEST_F(FastSampleEnvironmentReader, Verify) {
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(RawBuffer.get()), BufferSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}

TEST_F(FastSampleEnvironmentReader, VerifyFail) {
  auto TempData = std::make_unique<char[]>(BufferSize);
  std::memcpy(TempData.get(), RawBuffer.get(), BufferSize);
  FileWriter::FlatbufferMessage TestMessage1(
      reinterpret_cast<const char *>(TempData.get()), BufferSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage1));
  TempData[3] = 'h';
  EXPECT_THROW(FileWriter::FlatbufferMessage(
                   reinterpret_cast<const char *>(TempData.get()), BufferSize),
               FileWriter::NotValidFlatbuffer);
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

using FileWriter::HDFWriterModule_detail::InitResult;

TEST_F(FastSampleEnvironmentWriter, InitFile) {
  {
    senv::FastSampleEnvironmentWriter Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
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
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(FastSampleEnvironmentWriter, InitFileFail) {
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
}

TEST_F(FastSampleEnvironmentWriter, ReopenFileSuccess) {
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

using WriteResult = FileWriter::HDFWriterModule_detail::WriteResult;

TEST_F(FastSampleEnvironmentWriter, WriteDataOnce) {
  size_t BufferSize;
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
  EXPECT_TRUE(Writer.write(TestMsg) == WriteResult::OK);
  auto RawValuesDataset = UsedGroup.get_dataset("raw_value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  auto FbPointer = GetSampleEnvironmentData(TestMsg.data());

  auto DataspaceSize = RawValuesDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, FbPointer->Values()->size());
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  RawValuesDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), FbPointer->Values()->operator[](i));
  }

  auto NrOfTimeStampElements = TimestampDataset.dataspace().size();
  EXPECT_EQ(NrOfTimeStampElements, DataspaceSize);
  std::vector<std::uint64_t> TimestampsVector(NrOfTimeStampElements);
  TimestampDataset.read(TimestampsVector);
  for (int j = 0; j < DataspaceSize; j++) {
    EXPECT_EQ(TimestampsVector.at(j), FbPointer->Timestamps()->operator[](j));
  }

  std::vector<std::int32_t> CueIndex(1);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0);

  std::vector<std::uint32_t> CueTimestamp(1);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->PacketTimestamp());
}

TEST_F(FastSampleEnvironmentWriter, WriteDataTwice) {
  size_t BufferSize;
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
  EXPECT_TRUE(Writer.write(TestMsg) == WriteResult::OK);
  EXPECT_TRUE(Writer.write(TestMsg) == WriteResult::OK);
  auto RawValuesDataset = UsedGroup.get_dataset("raw_value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  auto FbPointer = GetSampleEnvironmentData(TestMsg.data());

  auto DataspaceSize = RawValuesDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, FbPointer->Values()->size() * 2);
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  RawValuesDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i),
              FbPointer->Values()->operator[](i % FbPointer->Values()->size()));
  }

  EXPECT_EQ(TimestampDataset.dataspace().size(), DataspaceSize);

  std::vector<std::uint32_t> CueIndex(2);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0u);
  EXPECT_EQ(CueIndex.at(1), FbPointer->Values()->size());

  std::vector<std::uint32_t> CueTimestamp(2);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->PacketTimestamp());
  EXPECT_EQ(CueTimestamp.at(1), FbPointer->PacketTimestamp());
}

TEST_F(FastSampleEnvironmentWriter, WriteNoElements) {
  size_t BufferSize;
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  auto FbPointer = GetSampleEnvironmentData(Buffer.get());
  auto ValueLengthPtr =
      reinterpret_cast<flatbuffers::uoffset_t *>(
          const_cast<std::uint8_t *>(FbPointer->Values()->Data())) -
      1;
  *ValueLengthPtr = 0;
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
  EXPECT_TRUE(Writer.write(TestMsg) == WriteResult::OK);
  auto RawValuesDataset = UsedGroup.get_dataset("raw_value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");
  EXPECT_EQ(RawValuesDataset.dataspace().size(), 0);
  EXPECT_EQ(TimestampDataset.dataspace().size(), 0);
  EXPECT_EQ(CueIndexDataset.dataspace().size(), 0);
  EXPECT_EQ(CueTimestampZeroDataset.dataspace().size(), 0);
}

TEST_F(FastSampleEnvironmentWriter, WriteDataWithNoTimestampsInFB) {
  size_t BufferSize;
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  auto FbPointer = GetSampleEnvironmentData(Buffer.get());
  auto TimestampsLengthPtr =
      reinterpret_cast<flatbuffers::uoffset_t *>(
          const_cast<std::uint8_t *>(FbPointer->Timestamps()->Data())) -
      1;
  *TimestampsLengthPtr = 0;
  senv::FastSampleEnvironmentWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
  EXPECT_TRUE(Writer.write(TestMsg) == WriteResult::OK);
  auto RawValuesDataset = UsedGroup.get_dataset("raw_value");
  auto TimestampDataset = UsedGroup.get_dataset("time");
  auto CueIndexDataset = UsedGroup.get_dataset("cue_index");
  auto CueTimestampZeroDataset = UsedGroup.get_dataset("cue_timestamp_zero");

  auto DataspaceSize = RawValuesDataset.dataspace().size();
  EXPECT_EQ(DataspaceSize, FbPointer->Values()->size());
  std::vector<std::uint16_t> AppendedValues(DataspaceSize);
  RawValuesDataset.read(AppendedValues);
  for (int i = 0; i < DataspaceSize; i++) {
    ASSERT_EQ(AppendedValues.at(i), FbPointer->Values()->operator[](i));
  }

  auto NrOfTimeStampElements = TimestampDataset.dataspace().size();
  EXPECT_EQ(NrOfTimeStampElements, DataspaceSize);
  std::vector<std::uint64_t> TimestampsVector(NrOfTimeStampElements);
  TimestampDataset.read(TimestampsVector);
  EXPECT_EQ(TimestampsVector.at(0), FbPointer->PacketTimestamp());

  std::vector<std::int32_t> CueIndex(1);
  EXPECT_NO_THROW(CueIndexDataset.read(CueIndex));
  EXPECT_EQ(CueIndex.at(0), 0);

  std::vector<std::uint32_t> CueTimestamp(1);
  EXPECT_NO_THROW(CueTimestampZeroDataset.read(CueTimestamp));
  EXPECT_EQ(CueTimestamp.at(0), FbPointer->PacketTimestamp());
}
