#include "../json.h"
#include "AddReader.h"
#include "FlatbufferMessage.h"
#include "schemas/ns10/NicosCacheWriter.h"
#include <flatbuffers/flatbuffers.h>
#include <fstream>
#include <gtest/gtest.h>

namespace ns10 {
#include "ns10_cache_entry_generated.h"
}

bool ns10dump(const flatbuffers::FlatBufferBuilder &Builder) {
  auto Verifier =
      flatbuffers::Verifier(Builder.GetBufferPointer(), Builder.GetSize());

  if (!ns10::VerifyCacheEntryBuffer(Verifier)) {
    return false;
  }
  auto CacheEntry = ns10::GetCacheEntry(Builder.GetBufferPointer());
  std::cout << "\tkey : " << CacheEntry->key()->str() << "\n";
  std::cout << "\tvalue : " << CacheEntry->value()->str() << "\n";
  std::cout << "\ttime : " << CacheEntry->time() << "\n";
  std::cout << "\tttl : " << CacheEntry->ttl() << "\n";
  std::cout << "\texpired : " << bool(CacheEntry->expired()) << "\n";
  return true;
}

FileWriter::FlatbufferMessage
createFlatbufferMessageFromJson(nlohmann::json Json) {
  double Time = 1.0;
  double Ttl = 1.0;
  uint8_t Expired = 0;
  std::string Key("");
  std::string Value("");

  if (auto Val = find<double>("time", Json)) {
    Time = Val.inner();
  }
  if (auto Val = find<std::string>("key", Json)) {
    Key = Val.inner();
  }
  if (auto Val = find<std::string>("value", Json)) {
    Value = Val.inner();
  }
  if (auto Val = find<double>("ttl", Json)) {
    Ttl = Val.inner();
  }
  if (auto Val = find<uint8_t>("expired", Json)) {
    Expired = Val.inner();
  }

  auto Builder = flatbuffers::FlatBufferBuilder();
  auto FBKey = Builder.CreateString(Key);
  auto FBValue = Builder.CreateString(Value);

  ns10::CacheEntryBuilder CEBuilder(Builder);

  CEBuilder.add_key(FBKey);
  CEBuilder.add_time(Time);
  CEBuilder.add_ttl(Ttl);
  CEBuilder.add_expired(Expired);
  CEBuilder.add_value(FBValue);

  FinishCacheEntryBuffer(Builder, CEBuilder.Finish());

  auto Message = FileWriter::FlatbufferMessage(
      reinterpret_cast<char *>(Builder.GetBufferPointer()), Builder.GetSize());

  return Message;
}

class NicosCacheReaderTest : public ::testing::Test {
public:
  void SetUp() override {
    nlohmann::json BufferJson = R"({
      "key": "nicos/device/parameter",
      "writer_module": "ns10",
      "time": 123.456,
      "value": "a string"
    })"_json;

    Message = std::make_unique<FileWriter::FlatbufferMessage>(
        createFlatbufferMessageFromJson(BufferJson));
  };

  void TearDown() override{};
  std::unique_ptr<FileWriter::FlatbufferMessage> Message;
};

TEST_F(NicosCacheReaderTest, ReaderReturnValues) {
  NicosCacheWriter::CacheReader SomeReader;
  EXPECT_TRUE(Message->isValid());
  EXPECT_EQ(Message->getSourceName(), std::string("nicos/device/parameter"));
  EXPECT_EQ(Message->getTimestamp(), 123.456 * 1e9);
}

class NicosCacheWriterTest : public ::testing::Test {

public:
  void SetUp() override {
    hdf5::property::FileCreationList fcpl;
    hdf5::property::FileAccessList fapl;
    // MemoryDriver(fapl);
    File = hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE,
                              fcpl, fapl);

    RootGroup = File.root();
    UsedGroup = RootGroup.create_group(NXLogGroup);
  };

  void TearDown() override { File.close(); };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  hdf5::file::File File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
  hdf5::file::MemoryDriver MemoryDriver;
};

// class NicosCacheWriterTest : public ::testing::Test {
// public:
//   static void SetUpTestCase() {
//     std::ifstream InFile(std::string(TEST_DATA_PATH) + "/someNDArray.data",
//                          std::ifstream::in | std::ifstream::binary);
//     InFile.seekg(0, InFile.end);
//     auto FileSize = InFile.tellg();
//     auto RawData = std::make_unique<char[]>(FileSize);
//     InFile.seekg(0, InFile.beg);
//     InFile.read(RawData.get(), FileSize);
//   };
//
//   static std::unique_ptr<char[]> RawData;
//   static size_t FileSize;
//
//   void SetUp() override {
//     hdf5::property::FileCreationList fcpl;
//     hdf5::property::FileAccessList fapl;
//     MemoryDriver(fapl);
//     File =
//         hdf5::file::create(TestFileName, hdf5::file::AccessFlags::TRUNCATE);
//         //,
//     fcpl, fapl);
//     RootGroup = File.root();
//     UsedGroup = RootGroup.create_group(NXLogGroup);
//   };
//
//   void TearDown() override { File.close(); };
//
//   hdf5::file::MemoryDriver MemoryDriver;
//   std::string TestFileName{"SomeTestFile.hdf5"};
//   std::string NXLogGroup{"SomeParentName"};
//   hdf5::file::File File;
//   hdf5::node::Group RootGroup;
//   hdf5::node::Group UsedGroup;
// };

class CacheWriterF : public NicosCacheWriter::CacheWriter {
public:
  using NicosCacheWriter::CacheWriter::ChunkSize;
  using NicosCacheWriter::CacheWriter::Timestamp;
  using NicosCacheWriter::CacheWriter::CueInterval;
  using NicosCacheWriter::CacheWriter::CueTimestamp;
  using NicosCacheWriter::CacheWriter::CueTimestampIndex;
  using NicosCacheWriter::CacheWriter::Values;
};

TEST_F(NicosCacheWriterTest, WriterReturnValues) {
  NicosCacheWriter::CacheWriter SomeWriter;
  EXPECT_TRUE(SomeWriter.init_hdf(UsedGroup, "{}") ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(SomeWriter.reopen(UsedGroup) ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_EQ(SomeWriter.flush(), 0);
  EXPECT_EQ(SomeWriter.close(), 0);
}

TEST_F(NicosCacheWriterTest, WriterInitCreateGroupTest) {
  NicosCacheWriter::CacheWriter SomeWriter;
  SomeWriter.init_hdf(UsedGroup, "{}");

  EXPECT_TRUE(UsedGroup.has_dataset("cue_index"));
  EXPECT_TRUE(UsedGroup.has_dataset("value"));
  EXPECT_TRUE(UsedGroup.has_dataset("time"));
  EXPECT_TRUE(UsedGroup.has_dataset("cue_timestamp_zero"));
  bool FoundAttribute{false};
  for (const auto &Attribute : UsedGroup.attributes) {
    if (Attribute.name() == "NX_class") {
      std::string ClassValue;
      Attribute.read(ClassValue);
      if (ClassValue == "NXlog") {
        FoundAttribute = true;
      }
    }
  }
  EXPECT_TRUE(FoundAttribute);
}

TEST_F(NicosCacheWriterTest, WriterTimeStampTest) {

  CacheWriterF Writer;

  Writer.init_hdf(UsedGroup, "{}");
  Writer.reopen(UsedGroup);

  nlohmann::json BufferJson = R"({
    "key": "nicos/device/parameter",
    "writer_module": "ns10",
    "time": 123.456,
    "value": "a string"
  })"_json;

  FileWriter::FlatbufferMessage Message(
      createFlatbufferMessageFromJson(BufferJson));

  {
    std::ofstream f("ns10.dat", std::ios::binary);
    f.write(Message.data(), Message.size());
    f.close();
  }

  std::vector<unsigned char> buf;
  {
    std::ifstream f("ns10.dat", std::ios::binary);
    std::vector<unsigned char> buffer(std::istreambuf_iterator<char>(f), {});
    f.close();
    buf = buffer;
    auto Verifier = flatbuffers::Verifier(
        reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
    std::cout << "Verifier: " << ns10::VerifyCacheEntryBuffer(Verifier) << "\n";
  }
  Writer.write(Message);

  auto Verifier = flatbuffers::Verifier(
      reinterpret_cast<const uint8_t *>(Message.data()), Message.size());
  std::cout << "Verifier: " << ns10::VerifyCacheEntryBuffer(Verifier) << "\n";

  // std::uint64_t storedTs{11111};
  // Writer.Timestamp.read(storedTs);
  // EXPECT_EQ(Message.getTimestamp(), storedTs);
}

// TEST_F(CacheWriterWriter, WriterInitString) {
//   NicosCacheWriter::CacheWriter Writer;
//   auto JsonConfig = nlohmann::json::parse(R""({
//     "type": "string"
//   })"");
//   Writer.parse_config(JsonConfig.dump(), "");
//   Writer.init_hdf(UsedGroup, "{}");
//   Writer.reopen(UsedGroup);
//   EXPECT_EQ(hdf5::datatype::create<std::string>(),
//   Writer.Values->datatype());
// }
