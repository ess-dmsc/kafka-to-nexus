// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include <memory>
#include <vs00_stringdata_generated.h>

#include "AccessMessageMetadata/vs00/vs00_Extractor.h"
#include "WriterModule/vs00/vs00_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

static std::unique_ptr<std::uint8_t[]>
GenerateStringdataFlatbufferData(size_t &BufferSize) {
  flatbuffers::FlatBufferBuilder builder;
  auto FBNameStringOffset = builder.CreateString("SomeTestString");
  AlarmBuilder MessageBuilder(builder);
  vs00_StringDataBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBNameStringOffset);
  MessageBuilder.add_timestamp(1655901153832343040);
  MessageBuilder.add_data(FBNameStringOffset);
  builder.Finish(MessageBuilder.Finish(), vs00_StringDataIdentifier());
  BufferSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::uint8_t[]>(BufferSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), BufferSize);
  return RawBuffer;
}

// using FBMsg = FileWriter::FlatbufferMessage;
using namespace WriterModule;

class vs00_StringWriter : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(NXtextlogGroup);
    setExtractorModule<AccessMessageMetadata::vs00_Extractor>("vs00");
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXtextlogGroup{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

using WriterModule::InitResult;

TEST_F(vs00_StringWriter, InitFile) {
  {
    WriterModule::vs00::vs00_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXtextlogGroup));
  auto TestGroup = RootGroup.get_group(NXtextlogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("time"));
  EXPECT_TRUE(TestGroup.has_dataset("value"));
}

TEST_F(vs00_StringWriter, ReopenFileFailure) {
  WriterModule::vs00::vs00_Writer Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(vs00_StringWriter, InitFileFail) {
  WriterModule::vs00::vs00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup) == InitResult::OK);
}

TEST_F(vs00_StringWriter, ReopenFileSuccess) {
  WriterModule::vs00::vs00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(vs00_StringWriter, WriteDataOnce) {
  size_t BufferSize{0};
  auto Buffer = GenerateStringdataFlatbufferData(BufferSize);
  WriterModule::vs00::vs00_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg, false));
  auto ValueDataset = UsedGroup.get_dataset("value");
  auto TimeDataset = UsedGroup.get_dataset("time");
  auto FbPointer = GetAlarm(TestMsg.data());

  std::vector<std::int64_t> Timestamp(1);
  EXPECT_NO_THROW(TimeDataset.read(Timestamp));
  EXPECT_EQ(FbPointer->timestamp(), Timestamp[0]);

  std::vector<std::string> StringMsg(1);
  EXPECT_NO_THROW(ValueDataset.read(StringMsg));
  StringMsg[0].erase(StringMsg[0].find('\0'));
  EXPECT_EQ(FbPointer->message()->str(), StringMsg[0]);
}
