#include "../schemas/ep00/FlatbufferReader.h"
#include "../schemas/ep00/ep00_rw.h"
#include "HDFWriterModule.h"
#include <gtest/gtest.h>

namespace FileWriter {
namespace Schemas {
namespace ep00 {
using nlohmann::json;

static std::unique_ptr<std::int8_t[]>
GenerateFlatbufferData(size_t &DataSize, uint64_t Timestamp, EventType Status) {
  flatbuffers::FlatBufferBuilder builder;
  auto source = builder.CreateString("SIMPLE:DOUBLE");
  EpicsConnectionInfoBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(source);
  MessageBuilder.add_timestamp(Timestamp);
  MessageBuilder.add_type(Status);
  builder.Finish(MessageBuilder.Finish(), "ep00");
  DataSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::int8_t[]>(DataSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), DataSize);
  return RawBuffer;
}

class ep00Tests : public ::testing::Test {
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

TEST_F(ep00Tests, file_init_ok) {
  {
    ep00::HDFWriterModule Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
                HDFWriterModule_detail::InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("alarm_status"));
  EXPECT_TRUE(TestGroup.has_dataset("alarm_time"));
}

TEST_F(ep00Tests, reopen_file) {
  ep00::HDFWriterModule Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) ==
               HDFWriterModule_detail::InitResult::OK);
}

TEST_F(ep00Tests, InitFileFail) {
  ep00::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
              HDFWriterModule_detail::InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup, "{}") ==
               HDFWriterModule_detail::InitResult::OK);
}

TEST_F(ep00Tests, ReopenFileSuccess) {
  ep00::HDFWriterModule Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup, "{}") ==
              HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) ==
              HDFWriterModule_detail::InitResult::OK);
}

TEST_F(ep00Tests, WriteDataOnce) {
  size_t BufferSize;
  uint64_t Timestamp = 5555555;
  auto Status = EventType::CONNECTED;
  std::unique_ptr<std::int8_t[]> Buffer =
      GenerateFlatbufferData(BufferSize, Timestamp, Status);
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
  auto TimeDataSet = UsedGroup.get_dataset("alarm_time");
  auto Size = TimeDataSet.dataspace().size();
  std::vector<uint64_t> Timestamps(Size);
  TimeDataSet.read(Timestamps);
  EXPECT_EQ(Timestamps[0], Timestamp);
}

TEST_F(ep00Tests, SuccessfulParseKB) {
  ep00::HDFWriterModule Writer;
  std::string Command = "{\"nexus\": {\n\"chunk\": {\n\"chunk_kb\": "
                        "1066\n},\n\"buffer\": {\n\"size_kb\": "
                        "512,\n\"packet_max_kb\": 128\n}\n}}";
  EXPECT_NO_THROW(Writer.parse_config(Command, ""));
}

TEST_F(ep00Tests, SuccessfulParseMB) {
  ep00::HDFWriterModule Writer;
  std::string Command = "{\"nexus\": {\n\"chunk\": {\n\"chunk_mb\": "
                        "1\n},\n\"buffer\": {\n\"size_mb\": "
                        "1,\n\"packet_max_kb\": 128\n}\n}}";
  EXPECT_NO_THROW(Writer.parse_config(Command, ""));
}

} // namespace ep00
} // namespace Schemas
} // namespace FileWriter
