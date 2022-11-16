// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <ep01_epics_connection_generated.h>
#include <gtest/gtest.h>
#include <memory>

#include "AccessMessageMetadata/ep01/ep01_Extractor.h"
#include "WriterModule/ep01/ep01_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

static std::unique_ptr<std::uint8_t[]>
GenerateConStatusFlatbufferData(size_t &BufferSize) {
  flatbuffers::FlatBufferBuilder builder;
  auto FBNameStringOffset = builder.CreateString("SomeTestString");
  EpicsPVConnectionInfoBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBNameStringOffset);
  MessageBuilder.add_timestamp(1655901153732343040);
  MessageBuilder.add_status(ConnectionInfo::DESTROYED);
  builder.Finish(MessageBuilder.Finish(), EpicsPVConnectionInfoIdentifier());
  BufferSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::uint8_t[]>(BufferSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), BufferSize);
  return RawBuffer;
}

// using FBMsg = FileWriter::FlatbufferMessage;
using namespace WriterModule;

class EPICS_ConStatusWriter : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(NXLogGroup);
    setExtractorModule<AccessMessageMetadata::ep01_Extractor>("ep01");
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

using WriterModule::InitResult;

TEST_F(EPICS_ConStatusWriter, InitFile) {
  {
    ep01::ep01_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("connection_status_time"));
  EXPECT_TRUE(TestGroup.has_dataset("connection_status"));
}

TEST_F(EPICS_ConStatusWriter, ReopenFileFailure) {
  ep01::ep01_Writer Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(EPICS_ConStatusWriter, InitFileFail) {
  ep01::ep01_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup) == InitResult::OK);
}

TEST_F(EPICS_ConStatusWriter, ReopenFileSuccess) {
  ep01::ep01_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(EPICS_ConStatusWriter, WriteDataOnce) {
  size_t BufferSize{0};
  auto Buffer = GenerateConStatusFlatbufferData(BufferSize);
  ep01::ep01_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto ConStatusTimeDataset = UsedGroup.get_dataset("connection_status_time");
  auto ConStatusDataset = UsedGroup.get_dataset("connection_status");
  auto FbPointer = GetEpicsPVConnectionInfo(TestMsg.data());

  std::vector<std::int64_t> Timestamp(1);
  EXPECT_NO_THROW(ConStatusTimeDataset.read(Timestamp));
  EXPECT_EQ(FbPointer->timestamp(), Timestamp[0]);

  std::vector<std::string> ConStatus(1);
  EXPECT_NO_THROW(ConStatusDataset.read(ConStatus));
  ConStatus[0].erase(ConStatus[0].find('\0'));
  EXPECT_EQ(std::string(EnumNameConnectionInfo(FbPointer->status())),
            ConStatus[0]);
}
