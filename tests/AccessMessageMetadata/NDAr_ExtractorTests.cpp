// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <NDAr_NDArray_schema_generated.h>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <gtest/gtest.h>

#include "AccessMessageMetadata/NDAr/NDAr_Extractor.h"
#include "helpers/SetExtractorModule.h"

class AreaDetectorReader : public ::testing::Test {
public:
  static void SetUpTestCase() {
    std::ifstream InFile(std::string(TEST_DATA_PATH) + "/someNDArray.data",
                         std::ifstream::in | std::ifstream::binary);
    InFile.seekg(0, InFile.end);
    FileSize = InFile.tellg();
    RawData = std::make_unique<uint8_t[]>(FileSize);
    InFile.seekg(0, InFile.beg);
    InFile.read(reinterpret_cast<char *>(RawData.get()), FileSize);
  };

  void SetUp() override {
    ASSERT_NE(FileSize, size_t(0));
    Reader = std::make_unique<AccessMessageMetadata::NDAr_Extractor>();
    setExtractorModule<AccessMessageMetadata::NDAr_Extractor>("NDAr");
  };

  std::unique_ptr<AccessMessageMetadata::NDAr_Extractor> Reader;
  static std::unique_ptr<uint8_t[]> RawData;
  static size_t FileSize;
};

std::unique_ptr<uint8_t[]> AreaDetectorReader::RawData;
size_t AreaDetectorReader::FileSize = 0;

TEST_F(AreaDetectorReader, ValidateTestOk) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  EXPECT_TRUE(Reader->verify(Message));
}

TEST_F(AreaDetectorReader, ValidateTestFail) {
  flatbuffers::FlatBufferBuilder builder;
  auto epics_ts = FB_Tables::epicsTimeStamp(0, 0);
  auto someDims = builder.CreateVector(std::vector<std::uint64_t>({
      0,
      1,
      2,
      3,
  }));
  auto someData = builder.CreateVector(std::vector<std::uint8_t>({
      0,
      1,
      2,
      3,
  }));
  auto tmpPkg = FB_Tables::CreateNDArray(builder, 0, 0, &epics_ts, someDims,
                                         FB_Tables::DType::Uint8, someData);
  builder.Finish(tmpPkg); // Finish without file identifier will fail verify

  EXPECT_THROW(FileWriter::FlatbufferMessage(builder.GetBufferPointer(),
                                             builder.GetSize()),
               std::runtime_error);
}

// We are currently using a static source name, this should be changed
// eventually
TEST_F(AreaDetectorReader, SourceNameTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  EXPECT_EQ(Reader->source_name(Message), "ADPluginKafka");
}

TEST_F(AreaDetectorReader, TimeStampTest) {
  FileWriter::FlatbufferMessage Message(RawData.get(), FileSize);
  auto tempNDArr = FB_Tables::GetNDArray(RawData.get());
  EXPECT_NE(tempNDArr->epicsTS()->secPastEpoch(), 0);
  EXPECT_NE(tempNDArr->epicsTS()->nsec(), 0);
  std::uint64_t unixEpicsSecDiff = 631152000;
  std::uint64_t secToNsec = 1000000000;
  std::uint64_t tempTimeStamp =
      (tempNDArr->epicsTS()->secPastEpoch() + unixEpicsSecDiff) * secToNsec;
  tempTimeStamp += tempNDArr->epicsTS()->nsec();
  EXPECT_EQ(Reader->timestamp(Message), tempTimeStamp);
}
