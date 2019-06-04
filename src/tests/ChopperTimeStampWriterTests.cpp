#include "schemas/tdct/ChopperTimeStampWriter.h"
#include "tdct_timestamps_generated.h"
#include <gtest/gtest.h>
#include <memory>

static std::unique_ptr<std::int8_t[]> GenerateFlatbufferData(size_t &DataSize) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::uint64_t> TestTimestamps{11, 22, 33, 44, 55, 66};
  auto FBTimestampOffset = builder.CreateVector(TestTimestamps);
  auto FBNameStringOffset = builder.CreateString("SomeTestString");
  timestampBuilder MessageBuilder(builder);
  MessageBuilder.add_name(FBNameStringOffset);
  MessageBuilder.add_timestamps(FBTimestampOffset);
  builder.Finish(MessageBuilder.Finish(), timestampIdentifier());
  DataSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::int8_t[]>(DataSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), DataSize);
  return RawBuffer;
}

using FileWriter::FlatbufferReaderRegistry::ReaderPtr;

class ChopperTimeStampGuard : public ::testing::Test {
public:
  static void SetUpTestCase() {
    RawBuffer = GenerateFlatbufferData(BufferSize);
  };

  void SetUp() override {
    ASSERT_NE(RawBuffer.get(), nullptr);
    ReaderUnderTest = std::make_unique<tdct::ChopperTimeStampGuard>();
    std::map<std::string, ReaderPtr> &Readers =
        FileWriter::FlatbufferReaderRegistry::getReaders();
    Readers.clear();
    FileWriter::FlatbufferReaderRegistry::Registrar<tdct::ChopperTimeStampGuard>
        RegisterIt("tdct");
  };

  std::unique_ptr<tdct::ChopperTimeStampGuard> ReaderUnderTest;
  static std::unique_ptr<std::int8_t[]> RawBuffer;
  static size_t BufferSize;
};
std::unique_ptr<std::int8_t[]> ChopperTimeStampGuard::RawBuffer{nullptr};
size_t ChopperTimeStampGuard::BufferSize{0};

TEST_F(ChopperTimeStampGuard, GetSourceName) {
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(RawBuffer.get()), BufferSize);
  EXPECT_EQ(ReaderUnderTest->source_name(TestMessage), "SomeTestString");
}

TEST_F(ChopperTimeStampGuard, GetTimeStamp) {
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(RawBuffer.get()), BufferSize);
  EXPECT_EQ(ReaderUnderTest->timestamp(TestMessage), 11u);
}

TEST_F(ChopperTimeStampGuard, Verify) {
  FileWriter::FlatbufferMessage TestMessage(
      reinterpret_cast<const char *>(RawBuffer.get()), BufferSize);
  EXPECT_TRUE(ReaderUnderTest->verify(TestMessage));
}

TEST_F(ChopperTimeStampGuard, VerifyFail) {
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

class ChopperTimeStampWriter : public ::testing::Test {
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

TEST_F(ChopperTimeStampWriter, InitFile) {
  {
    tdct::ChopperTimeStampWriter Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("cue_index"));
  EXPECT_TRUE(TestGroup.has_dataset("time"));
  EXPECT_TRUE(TestGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(ChopperTimeStampWriter, ReopenFileFailure) {
  tdct::ChopperTimeStampWriter Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(ChopperTimeStampWriter, InitFileFail) {
  tdct::ChopperTimeStampWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
}

TEST_F(ChopperTimeStampWriter, ReopenFileSuccess) {
  tdct::ChopperTimeStampWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(ChopperTimeStampWriter, WriteDataOnce) {
  size_t BufferSize;
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  tdct::ChopperTimeStampWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
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
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  tdct::ChopperTimeStampWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
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
  std::unique_ptr<std::int8_t[]> Buffer = GenerateFlatbufferData(BufferSize);
  auto FbPointer = Gettimestamp(Buffer.get());
  auto ValueLengthPtr =
      reinterpret_cast<flatbuffers::uoffset_t *>(
          const_cast<std::uint8_t *>(FbPointer->timestamps()->Data())) -
      1;
  *ValueLengthPtr = 0;
  tdct::ChopperTimeStampWriter Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  EXPECT_THROW(FileWriter::FlatbufferMessage(
                   reinterpret_cast<const char *>(Buffer.get()), BufferSize),
               std::runtime_error);
}
