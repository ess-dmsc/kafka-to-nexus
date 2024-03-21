// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>
#include <memory>

#include "AccessMessageMetadata/ns10/ns10_Extractor.h"
#include "FlatbufferMessage.h"
#include "WriterModule/ns10/ns10_Writer.h"
#include "WriterRegistrar.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"
#include "json.h"
#include <ns10_cache_entry_generated.h>

using WriterModule::ns10::ns10_Writer;

std::unique_ptr<flatbuffers::FlatBufferBuilder>
createFlatbufferMessageFromJson(nlohmann::json const &Json) {
  double Time = 1.0;
  double Ttl = 1.0;
  uint8_t Expired = 0;
  std::string Key;
  std::string Value;

  if (auto Val = find<double>("time", Json)) {
    Time = *Val;
  }
  if (auto Val = find<std::string>("key", Json)) {
    Key = *Val;
  }
  if (auto Val = find<std::string>("value", Json)) {
    Value = *Val;
  }
  if (auto Val = find<double>("ttl", Json)) {
    Ttl = *Val;
  }
  if (auto Val = find<uint8_t>("expired", Json)) {
    Expired = *Val;
  }

  auto Builder = std::make_unique<flatbuffers::FlatBufferBuilder>();
  auto FBKey = Builder->CreateString(Key);
  auto FBValue = Builder->CreateString(Value);

  CacheEntryBuilder CEBuilder(*Builder);

  CEBuilder.add_value(FBValue);
  CEBuilder.add_expired(Expired);
  CEBuilder.add_time(Time);
  CEBuilder.add_ttl(Ttl);
  CEBuilder.add_key(FBKey);

  FinishCacheEntryBuffer(*Builder, CEBuilder.Finish());

  return Builder;
}

void registerSchema() {
  try {
    WriterModule::Registry::Registrar<ns10_Writer> RegisterIt(
        "ns10", "another_test_name");
  } catch (...) {
  }
}

class NicosCacheWriterTest : public ::testing::Test {

public:
  void SetUp() override {
    registerSchema();

    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(NXLogGroup);
    setExtractorModule<AccessMessageMetadata::ns10_Extractor>("ns10");
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
  hdf5::file::MemoryDriver MemoryDriver;
};

class CacheWriterF : public ns10_Writer {
public:
  using ns10_Writer::ChunkSize;
  using ns10_Writer::CueInterval;
  using ns10_Writer::CueTimestamp;
  using ns10_Writer::CueTimestampIndex;
  using ns10_Writer::SourceName;
  using ns10_Writer::Timestamp;
  using ns10_Writer::Values;
};

TEST_F(NicosCacheWriterTest, WriterReturnValues) {
  ns10_Writer SomeWriter;
  EXPECT_TRUE(SomeWriter.init_hdf(UsedGroup) == WriterModule::InitResult::OK);
  EXPECT_TRUE(SomeWriter.reopen(UsedGroup) == WriterModule::InitResult::OK);
}

TEST_F(NicosCacheWriterTest, WriterInitCreateGroupTest) {
  ns10_Writer SomeWriter;
  SomeWriter.init_hdf(UsedGroup);

  EXPECT_TRUE(UsedGroup.has_dataset("cue_index"));
  EXPECT_TRUE(UsedGroup.has_dataset("value"));
  EXPECT_TRUE(UsedGroup.has_dataset("time"));
  EXPECT_TRUE(UsedGroup.has_dataset("cue_timestamp_zero"));
}

TEST_F(NicosCacheWriterTest, WriterConfiguration) {
  nlohmann::json JsonConfig = R"({
    "source" : "nicos/device/parameter",
    "cue_interval": 1024,
    "chunk_size": 128
  })"_json;

  CacheWriterF Writer;
  Writer.parse_config(JsonConfig.dump());
  EXPECT_EQ(Writer.SourceName, JsonConfig["source"]);
  EXPECT_EQ(Writer.ChunkSize, JsonConfig["chunk_size"].get<uint64_t>());
  EXPECT_EQ(Writer.CueInterval, JsonConfig["cue_interval"].get<int>());
}

TEST_F(NicosCacheWriterTest, WriteTimeStamp) {
  nlohmann::json JsonConfig = R"({
    "source" : "nicos/device/parameter"
  })"_json;

  CacheWriterF Writer;
  Writer.parse_config(JsonConfig.dump());

  Writer.init_hdf(UsedGroup);
  Writer.reopen(UsedGroup);

  nlohmann::json BufferJson = R"({
    "key": "nicos/device/parameter",
    "WriterModule": "ns10",
    "time": 123.456,
    "value": "10.01"
  })"_json;

  auto Builder = createFlatbufferMessageFromJson(BufferJson);
  auto Message = FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                               Builder->GetSize());

  Writer.write(Message);

  uint64_t storedTs{11111};
  Writer.Timestamp.read_data(storedTs);
  EXPECT_EQ(storedTs, 123456000000ul);
}

TEST_F(NicosCacheWriterTest, WriteValues) {
  nlohmann::json JsonConfig = R"({
    "source" : "nicos/device/parameter"
  })"_json;

  CacheWriterF Writer;
  Writer.parse_config(JsonConfig.dump());

  Writer.init_hdf(UsedGroup);
  Writer.reopen(UsedGroup);

  nlohmann::json BufferJson = R"({
    "key": "nicos/device/parameter",
    "WriterModule": "ns10",
    "time": 123.456,
    "value": "10.01"
  })"_json;

  auto Builder = createFlatbufferMessageFromJson(BufferJson);
  auto Message = FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                               Builder->GetSize());

  Writer.write(Message);

  double storedValue;
  Writer.Values.read_data(storedValue);
  EXPECT_EQ(10.01, storedValue);
}

TEST_F(NicosCacheWriterTest, UpdateCueIndex) {
  nlohmann::json JsonConfig = R"({
    "source" : "nicos/device/parameter",
    "cue_interval": 10
  })"_json;

  CacheWriterF Writer;
  Writer.parse_config(JsonConfig.dump());

  Writer.init_hdf(UsedGroup);
  Writer.reopen(UsedGroup);

  nlohmann::json BufferJson = R"({
    "key": "nicos/device/parameter",
    "WriterModule": "ns10",
    "time": 123.456,
    "value": "10.01"
  })"_json;

  for (uint64_t i = 0; i < 10; ++i) {
    auto Builder = createFlatbufferMessageFromJson(BufferJson);
    auto Message = FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                                 Builder->GetSize());
    Writer.write(Message);
  }

  uint32_t Index;
  EXPECT_NO_THROW(Writer.CueTimestampIndex.read_data(Index));
}

TEST_F(NicosCacheWriterTest, ThrowsIfValueCannotBeCastToDouble) {
  nlohmann::json JsonConfig = R"({
    "source" : "nicos/device/parameter"
  })"_json;

  CacheWriterF Writer;
  Writer.parse_config(JsonConfig.dump());

  Writer.init_hdf(UsedGroup);
  Writer.reopen(UsedGroup);

  nlohmann::json BufferJson = R"({
    "key": "nicos/device/parameter",
    "WriterModule": "ns10",
    "time": 123.456,
    "value": "This is a string"
  })"_json;

  auto Builder = createFlatbufferMessageFromJson(BufferJson);
  auto Message = FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                               Builder->GetSize());

  EXPECT_THROW(Writer.write(Message), std::invalid_argument);
}

TEST_F(NicosCacheWriterTest, ThrowsIfValueDoesNotFitIntoDouble) {
  nlohmann::json JsonConfig = R"({
    "source" : "nicos/device/parameter"
  })"_json;

  CacheWriterF Writer;
  Writer.parse_config(JsonConfig.dump());

  Writer.init_hdf(UsedGroup);
  Writer.reopen(UsedGroup);

  nlohmann::json BufferJson = R"({
    "key": "nicos/device/parameter",
    "WriterModule": "ns10",
    "time": 123.456,
    "value": "2e1024"
  })"_json;

  auto Builder = createFlatbufferMessageFromJson(BufferJson);
  auto Message = FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                               Builder->GetSize());
  EXPECT_THROW(Writer.write(Message), std::out_of_range);
}
