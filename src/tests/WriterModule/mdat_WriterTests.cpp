// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <gtest/gtest.h>
#include <h5cpp/hdf5.hpp>
#include <memory>

#include "AccessMessageMetadata/mdat/mdat_Extractor.h"
#include "FlatbufferMessage.h"
#include "WriterModule/mdat/mdat_Writer.h"
#include "helper.h"
#include "helpers/HDFFileTestHelper.h"
#include "helpers/SetExtractorModule.h"

using nlohmann::json;

using namespace WriterModule::mdat;

class mdatInit : public ::testing::Test {
public:
  void SetUp() override {
    TestFile = HDFFileTestHelper::createInMemoryTestFile(TestFileName, false);
    RootGroup = TestFile->hdfGroup();
    setExtractorModule<AccessMessageMetadata::mdat_Extractor>("mdat");
  }
  std::unique_ptr<HDFFileTestHelper::DebugHDFFile> TestFile;
  hdf5::node::Group RootGroup;
  std::string TestFileName{"SomeTestFile.hdf5"};
};

class mdat_WriterStandIn : public mdat_Writer {
public:
  using mdat_Writer::ChunkSize;
  using mdat_Writer::mdatEnd_datetime;
  using mdat_Writer::mdatStart_datetime;
  using mdat_Writer::StringSize;
};

TEST_F(mdatInit, BasicDefaultInit) {
  mdat_Writer TestWriter;
  TestWriter.init_hdf(RootGroup);
  EXPECT_TRUE(RootGroup.has_dataset("start_time"));
}

TEST_F(mdatInit, ReOpenSuccess) {
  mdat_Writer TestWriter;
  TestWriter.init_hdf(RootGroup);
  EXPECT_EQ(TestWriter.reopen(RootGroup), WriterModule::InitResult::OK);
}

TEST_F(mdatInit, ReOpenFailure) {
  mdat_Writer TestWriter;
  EXPECT_EQ(TestWriter.reopen(RootGroup), WriterModule::InitResult::ERROR);
}

TEST_F(mdatInit, CheckInitDataType) {
  mdat_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  NeXusDataset::DateTime Value(RootGroup, "start_time",
                               NeXusDataset::Mode::Open, TestWriter.StringSize,
                               TestWriter.ChunkSize);
  hdf5::datatype::String StringType =
      hdf5::datatype::String::fixed(TestWriter.StringSize);
  StringType.encoding(hdf5::datatype::CharacterEncoding::UTF8);
  StringType.padding(hdf5::datatype::StringPad::NullTerm);
  EXPECT_EQ(Value.datatype(), StringType.native_type());
}

class mdatConfigParse : public ::testing::Test {
public:
};

TEST_F(mdatInit, WriteOneElement) {
  mdat_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  time_point Timestamp{std::chrono::seconds{1}};

  EXPECT_EQ(TestWriter.mdatStart_datetime.dataspace().size(), 0);
  TestWriter.writemetadata("start_time", Timestamp);
  ASSERT_EQ(TestWriter.mdatStart_datetime.dataspace().size(), 1);
  std::vector<std::string> WrittenTimes(1);
  TestWriter.mdatStart_datetime.read(WrittenTimes);
  //  timezones not existing on all Jenkins builds
  EXPECT_EQ(WrittenTimes.at(0).substr(0, 21),
            std::string("1970-01-01T00:00:01Z+0100").substr(0, 21));
}
