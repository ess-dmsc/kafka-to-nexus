#include "AccessMessageMetadata/ep00/ep00_Extractor.h"
#include "WriterModule/ep00/ep00_Writer.h"
#include "helpers/SetExtractorModule.h"
#include <ep00_epics_connection_info_generated.h>
#include <gtest/gtest.h>

namespace {
const std::string StatusName = "connection_status";
const std::string TimestampName = "connection_status_time";

} // namespace

namespace WriterModule {
namespace ep00 {

static std::unique_ptr<std::int8_t[]>
GenerateFlatbufferData(size_t &DataSize, const uint64_t Timestamp,
                       const EventType Status, const std::string &SourceName) {
  flatbuffers::FlatBufferBuilder builder;

  auto Source = builder.CreateString(SourceName);
  EpicsConnectionInfoBuilder MessageBuilder(builder);
  if (!SourceName.empty()) {
    MessageBuilder.add_source_name(Source);
  }
  MessageBuilder.add_timestamp(Timestamp);
  MessageBuilder.add_type(Status);
  builder.Finish(MessageBuilder.Finish(), "ep00");
  DataSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::int8_t[]>(DataSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), DataSize);
  return RawBuffer;
}

class Schema_ep00 : public ::testing::Test {
public:
  void SetUp() override {
    setExtractorModule<AccessMessageMetadata::ep00_Extractor>("ep00");
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::Truncate);
    RootGroup = File.root();
    UsedGroup = RootGroup.create_group(NXLogGroup);
  }
  void TearDown() override { File.close(); };

  std::string NXLogGroup{"SomeParentName"};
  std::string TestFileName{"SomeTestFile.hdf5"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

// cppcheck-suppress syntaxError
TEST_F(Schema_ep00, FileInitOk) {
  WriterModule::ep00::ep00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset(StatusName));
  EXPECT_TRUE(TestGroup.has_dataset(TimestampName));
}

TEST_F(Schema_ep00, ReopenFile) {
  WriterModule::ep00::ep00_Writer Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(Schema_ep00, FileInitFailIfInitialisedTwice) {
  WriterModule::ep00::ep00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup) == InitResult::OK);
}

TEST_F(Schema_ep00, ReopenFileSuccess) {
  WriterModule::ep00::ep00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(Schema_ep00, WriteDataSuccess) {

  size_t BufferSize;
  uint64_t Timestamp = 5555555;
  std::string SourceName = "SIMPLE:DOUBLE";
  auto Status = EventType::CONNECTED;
  std::unique_ptr<std::int8_t[]> Buffer =
      GenerateFlatbufferData(BufferSize, Timestamp, Status, SourceName);
  WriterModule::ep00::ep00_Writer Writer;
  {
    Writer.init_hdf(UsedGroup);
    Writer.reopen(UsedGroup);
  }
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<uint8_t const *>(Buffer.get()), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto TimeDataSet = UsedGroup.get_dataset(TimestampName);
  auto Size = TimeDataSet.dataspace().size();
  std::vector<uint64_t> Timestamps(Size);
  TimeDataSet.read(Timestamps);
  EXPECT_EQ(Timestamps[0], Timestamp);

  auto StatusDataset = UsedGroup.get_dataset(StatusName);
  std::vector<std::int16_t> StatusData(StatusDataset.dataspace().size());
  EXPECT_NO_THROW(StatusDataset.read(StatusData));
  EXPECT_EQ(static_cast<EventType>(StatusData[0]), Status);
}

TEST_F(Schema_ep00, ConnectionStatusStoredAsInteger) {
  size_t BufferSize;
  uint64_t Timestamp = 5555555;
  std::string SourceName = "SIMPLE:DOUBLE";
  auto Status = EventType::CONNECTED;
  std::unique_ptr<std::int8_t[]> Buffer =
      GenerateFlatbufferData(BufferSize, Timestamp, Status, SourceName);
  WriterModule::ep00::ep00_Writer Writer;
  {
    Writer.init_hdf(UsedGroup);
    Writer.reopen(UsedGroup);
  }
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<uint8_t const *>(Buffer.get()), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));

  auto StatusDataset = UsedGroup.get_dataset(StatusName);

  ASSERT_EQ(StatusDataset.datatype().get_class(),
            hdf5::datatype::create<short>().get_class());
}

} // namespace ep00
} // namespace WriterModule
