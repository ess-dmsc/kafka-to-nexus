#include "../schemas/ep00/FlatbufferReader.h"
#include "../schemas/ep00/ep00_rw.h"
#include "HDFWriterModule.h"
#include <gtest/gtest.h>

namespace {
const std::string StatusName = "connection_status";
const std::string TimestampName = "connection_status_time";
}

namespace FileWriter {
namespace Schemas {
namespace ep00 {
using nlohmann::json;

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
    try {
      FileWriter::FlatbufferReaderRegistry::Registrar<
          FileWriter::Schemas::ep00::FlatbufferReader>
          RegisterIt("ep00");
    } catch (...) {
    }
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE);
    RootGroup = File.root();
    UsedGroup = RootGroup.create_group(NXLogGroup);
  }
  void TearDown() override { File.close(); };

  std::string NXLogGroup{"SomeParentName"};
  std::string TestFileName{"SomeTestFile.hdf5"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
  SharedLogger Logger = getLogger();
};

// cppcheck-suppress syntaxError
TEST_F(Schema_ep00, FileInitOk) {
  ep00::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
              HDFWriterModule_detail::InitResult::OK);
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset(StatusName));
  EXPECT_TRUE(TestGroup.has_dataset(TimestampName));
}

TEST_F(Schema_ep00, ReopenFile) {
  ep00::HDFWriterModule Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) ==
               HDFWriterModule_detail::InitResult::OK);
}

TEST_F(Schema_ep00, FileInitFail) {
  ep00::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
              HDFWriterModule_detail::InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup, "{}") ==
               HDFWriterModule_detail::InitResult::OK);
}

TEST_F(Schema_ep00, ReopenFileSuccess) {
  ep00::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
              HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) ==
              HDFWriterModule_detail::InitResult::OK);
}

TEST_F(Schema_ep00, WriteDataSuccess) {

  size_t BufferSize;
  uint64_t Timestamp = 5555555;
  std::string SourceName = "SIMPLE:DOUBLE";
  auto Status = EventType::CONNECTED;
  std::unique_ptr<std::int8_t[]> Buffer =
      GenerateFlatbufferData(BufferSize, Timestamp, Status, SourceName);
  ep00::HDFWriterModule Writer;
  {
    EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
                HDFWriterModule_detail::InitResult::OK);
    EXPECT_TRUE(Writer.reopen(UsedGroup) ==
                HDFWriterModule_detail::InitResult::OK);
  }
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto TimeDataSet = UsedGroup.get_dataset(TimestampName);
  auto Size = TimeDataSet.dataspace().size();
  std::vector<uint64_t> Timestamps(Size);
  TimeDataSet.read(Timestamps);
  EXPECT_EQ(Timestamps[0], Timestamp);

  auto StatusDataset = UsedGroup.get_dataset(StatusName);
  std::vector<std::string> StatusData(StatusDataset.dataspace().size());
  auto Datatype = hdf5::datatype::String::variable();
  Datatype.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  auto Dataspace = hdf5::dataspace::Simple({1});
  EXPECT_NO_THROW(StatusDataset.read(StatusData, Datatype, Dataspace));
  EXPECT_EQ(StatusData[0], "CONNECTED");
}

TEST_F(Schema_ep00, SuccessfulParseKB) {
  ep00::HDFWriterModule Writer;
  auto Chunk = 42;
  auto Buffer = 42;
  auto Packet = 42;
  std::string Command =
      fmt::format("{{\"nexus\": {{\n\"chunk\": {{\n\"chunk_kb\": "
                  "{}\n}},\n\"buffer\": {{\n\"size_kb\": "
                  "{},\n\"packet_max_kb\": {}\n}}\n}}}}",
                  Chunk, Buffer, Packet);
  EXPECT_NO_THROW(Writer.parse_config(Command, ""));
  EXPECT_EQ(Writer.BufferPacketMax, Packet * 1024U);
  EXPECT_EQ(Writer.ChunkBytes, Chunk * 1024U);
  EXPECT_EQ(Writer.BufferSize, Buffer * 1024U);
}

TEST_F(Schema_ep00, SuccessfulParseMB) {
  ep00::HDFWriterModule Writer;
  auto Chunk = 42;
  auto Buffer = 42;
  std::string Command =
      fmt::format("{{\"nexus\": {{\n\"chunk\": {{\n\"chunk_mb\": "
                  "{}\n}},\n\"buffer\": {{\n\"size_mb\": "
                  "{}\n}}\n}}}}",
                  Chunk, Buffer);
  EXPECT_NO_THROW(Writer.parse_config(Command, ""));
  EXPECT_EQ(Writer.ChunkBytes, Chunk * 1024 * 1024U);
  EXPECT_EQ(Writer.BufferSize, Buffer * 1024 * 1024U);
}

TEST_F(Schema_ep00, SuccessfulFlushAndClose) {
  ep00::HDFWriterModule Writer;
  EXPECT_EQ(0, Writer.flush());
  EXPECT_EQ(0, Writer.close());
}

TEST_F(Schema_ep00, FBReaderNoSourceName) {
  size_t BufferSize;
  uint64_t Timestamp = 5555555;
  std::string SourceName = "";
  auto Status = EventType::NEVER_CONNECTED;
  std::unique_ptr<std::int8_t[]> Buffer =
      GenerateFlatbufferData(BufferSize, Timestamp, Status, SourceName);
  ep00::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
              HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) ==
              HDFWriterModule_detail::InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(
      reinterpret_cast<const char *>(Buffer.get()), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto TimeDataSet = UsedGroup.get_dataset(TimestampName);
  auto Size = TimeDataSet.dataspace().size();
  std::vector<uint64_t> Timestamps(Size);
  TimeDataSet.read(Timestamps);
  EXPECT_EQ(Timestamps[0], Timestamp);

  auto StatusDataset = UsedGroup.get_dataset(StatusName);
  std::vector<std::string> StatusData(StatusDataset.dataspace().size());
  auto Datatype = hdf5::datatype::String::variable();
  Datatype.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  auto Dataspace = hdf5::dataspace::Simple({1});
  EXPECT_NO_THROW(StatusDataset.read(StatusData, Datatype, Dataspace));
  EXPECT_EQ(StatusData[0], "NEVER_CONNECTED");
}

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
