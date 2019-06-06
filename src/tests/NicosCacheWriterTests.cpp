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
createFlatbufferMessageFromJson(nlohmann::json j) {
  double Time = 1.0;
  double Ttl = 1.0;
  uint8_t Expired = 0;
  std::string Key("");
  std::string Value("");

  std::cout << j << "\n";

  if (auto Val = find<double>("time", j)) {
    Time = Val.inner();
  }
  if (auto Val = find<std::string>("key", j)) {
    Key = Val.inner();
  }
  if (auto Val = find<std::string>("value", j)) {
    Value = Val.inner();
  }
  if (auto Val = find<double>("ttl", j)) {
    Ttl = Val.inner();
  }
  if (auto Val = find<uint8_t>("expired", j)) {
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

TEST(NicosCacheReaderTest, ReaderReturnValues) {
  NicosCacheWriter::ReaderClass SomeReader;
  nlohmann::json BufferJSON = R"({
    "key": "nicos/device/parameter",
    "writer_module": "ns10",
    "time": 123.456,
    "value": "a string"
  })"_json;

  auto FlatbufferMessage = createFlatbufferMessageFromJson(BufferJSON);

  EXPECT_TRUE(FlatbufferMessage.isValid());
  EXPECT_EQ(FlatbufferMessage.getSourceName(), std::string("nicos"));
  EXPECT_EQ(FlatbufferMessage.getTimestamp(), 123.456 * 1e9);
}

TEST(NicosCacheWriterTests, WriterReturnValues) {
  NicosCacheWriter::WriterClass SomeWriter;
  hdf5::node::Group SomeGroup;
  EXPECT_TRUE(SomeWriter.init_hdf(SomeGroup, "{}") ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(SomeWriter.reopen(SomeGroup) ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_NO_THROW(SomeWriter.write(FileWriter::FlatbufferMessage()));
  EXPECT_EQ(SomeWriter.flush(), 0);
  EXPECT_EQ(SomeWriter.close(), 0);
}

TEST(NicosCacheWriterTests, CreateGroupFrom) {

  NicosCacheWriter::WriterClass SomeWriter;
  hdf5::node::Group SomeGroup;
  EXPECT_TRUE(SomeWriter.init_hdf(SomeGroup, "{}") ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_TRUE(SomeWriter.reopen(SomeGroup) ==
              FileWriter::HDFWriterModule_detail::InitResult::OK);
  EXPECT_NO_THROW(SomeWriter.write(FileWriter::FlatbufferMessage()));
  EXPECT_EQ(SomeWriter.flush(), 0);
  EXPECT_EQ(SomeWriter.close(), 0);
}
