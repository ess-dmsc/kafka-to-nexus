// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <flatbuffers/flatbuffers.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

#include "AccessMessageMetadata/ns10/ns10_Extractor.h"
#include "FlatbufferMessage.h"
#include "helpers/SetExtractorModule.h"
#include "json.h"
#include "ns10_cache_entry_generated.h"

static std::unique_ptr<flatbuffers::FlatBufferBuilder>
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

class NicosCacheReaderTest : public ::testing::Test {
public:
  void SetUp() override {
    setExtractorModule<AccessMessageMetadata::ns10_Extractor>("ns10");
  };
};

TEST_F(NicosCacheReaderTest, ReaderReturnValues) {
  nlohmann::json BufferJson = R"({
      "key": "nicos/device/parameter",
      "WriterModule": "ns10",
      "time": 123.456,
      "value": "10.01"
    })"_json;

  auto Builder = createFlatbufferMessageFromJson(BufferJson);
  auto Message = FileWriter::FlatbufferMessage(Builder->GetBufferPointer(),
                                               Builder->GetSize());

  EXPECT_TRUE(Message.isValid());
  EXPECT_EQ(Message.getSourceName(), std::string("nicos/device/parameter"));
  EXPECT_EQ(Message.getTimestamp(), 123.456 * 1e9);
}