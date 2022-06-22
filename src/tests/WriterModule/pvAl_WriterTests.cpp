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
#include <pvAl_epics_pv_alarm_state_generated.h>

#include "AccessMessageMetadata/pvAl/pvAl_Extractor.h"
#include "WriterModule/pvAl/pvAl_Writer.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

static std::unique_ptr<std::uint8_t[]>
GenerateAlarmFlatbufferData(size_t &BufferSize) {
  flatbuffers::FlatBufferBuilder builder;
  auto FBNameStringOffset = builder.CreateString("SomeTestString");
  PV_AlarmStateBuilder MessageBuilder(builder);
  MessageBuilder.add_source_name(FBNameStringOffset);
  MessageBuilder.add_timestamp(1655901153832343040);
  MessageBuilder.add_severity(AlarmSeverity::MAJOR);
  MessageBuilder.add_state(AlarmState::CLIENT);
  builder.Finish(MessageBuilder.Finish(), PV_AlarmStateIdentifier());
  BufferSize = builder.GetSize();
  auto RawBuffer = std::make_unique<std::uint8_t[]>(BufferSize);
  std::memcpy(RawBuffer.get(), builder.GetBufferPointer(), BufferSize);
  return RawBuffer;
}

// using FBMsg = FileWriter::FlatbufferMessage;
using namespace WriterModule;

class EPICS_AlarmWriter : public ::testing::Test {
public:
  void SetUp() override {
    File = HDFFileTestHelper::createInMemoryTestFile(TestFileName);
    RootGroup = File->hdfGroup();
    UsedGroup = RootGroup.create_group(NXLogGroup);
    setExtractorModule<AccessMessageMetadata::pvAl_Extractor>("pvAl");
  };

  std::string TestFileName{"SomeTestFile.hdf5"};
  std::string NXLogGroup{"SomeParentName"};
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> File;
  hdf5::node::Group RootGroup;
  hdf5::node::Group UsedGroup;
};

using WriterModule::InitResult;

TEST_F(EPICS_AlarmWriter, InitFile) {
  {
    pvAl::pvAl_Writer Writer;
    EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  }
  ASSERT_TRUE(RootGroup.has_group(NXLogGroup));
  auto TestGroup = RootGroup.get_group(NXLogGroup);
  EXPECT_TRUE(TestGroup.has_dataset("alarm_status"));
  EXPECT_TRUE(TestGroup.has_dataset("alarm_severity"));
  EXPECT_TRUE(TestGroup.has_dataset("alarm_time"));
}

TEST_F(EPICS_AlarmWriter, ReopenFileFailure) {
  pvAl::pvAl_Writer Writer;
  EXPECT_FALSE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(EPICS_AlarmWriter, InitFileFail) {
  pvAl::pvAl_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_FALSE(Writer.init_hdf(UsedGroup) == InitResult::OK);
}

TEST_F(EPICS_AlarmWriter, ReopenFileSuccess) {
  pvAl::pvAl_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
}

TEST_F(EPICS_AlarmWriter, WriteDataOnce) {
  size_t BufferSize{0};
  auto Buffer = GenerateAlarmFlatbufferData(BufferSize);
  pvAl::pvAl_Writer Writer;
  EXPECT_TRUE(Writer.init_hdf(UsedGroup) == InitResult::OK);
  EXPECT_TRUE(Writer.reopen(UsedGroup) == InitResult::OK);
  FileWriter::FlatbufferMessage TestMsg(Buffer.get(), BufferSize);
  EXPECT_NO_THROW(Writer.write(TestMsg));
  auto AlarmStatusDataset = UsedGroup.get_dataset("alarm_status");
  auto AlarmSeverityDataset = UsedGroup.get_dataset("alarm_severity");
  auto AlarmTimeDataset = UsedGroup.get_dataset("alarm_time");
  auto FbPointer = GetPV_AlarmState(TestMsg.data());

  std::vector<std::int64_t> Timestamp(1);
  EXPECT_NO_THROW(AlarmTimeDataset.read(Timestamp));
  EXPECT_EQ(FbPointer->timestamp(), Timestamp[0]);

  std::vector<std::string> AlarmSeverity(1);
  EXPECT_NO_THROW(AlarmSeverityDataset.read(AlarmSeverity));
  AlarmSeverity[0].erase(AlarmSeverity[0].find('\0'));
  EXPECT_EQ(std::string(EnumNameAlarmSeverity(FbPointer->severity())),
            AlarmSeverity[0]);

  std::vector<std::string> AlarmsStatus(1);
  EXPECT_NO_THROW(AlarmStatusDataset.read(AlarmsStatus));
  AlarmsStatus[0].erase(AlarmsStatus[0].find('\0'));
  EXPECT_EQ(std::string(EnumNameAlarmState(FbPointer->state())),
            AlarmsStatus[0]);
}
