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
  using mdat_Writer::mdatStart_time;
  using mdat_Writer::mdatStop_time;
  using mdat_Writer::ChunkSize;
};

TEST_F(mdatInit, BasicDefaultInit) {
  mdat_Writer TestWriter;
  TestWriter.init_hdf(RootGroup);
  EXPECT_TRUE(RootGroup.has_dataset("time"));
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
  auto Open = NeXusDataset::Mode::Open;
  NeXusDataset::Time Value(RootGroup, Open);
  EXPECT_EQ(Value.datatype(), hdf5::datatype::create<uint64_t>());
}

class mdatConfigParse : public ::testing::Test {
public:
};

namespace mdat_schema {
std::pair<std::unique_ptr<uint8_t[]>, size_t> generateFlatbufferMessage(std::uint64_t Timestamp) {
  flatbuffers::FlatBufferBuilder builder;
  auto startName = builder.CreateString("start_time");
  flatbuffers::uoffset_t start_ = builder.StartTable();
  builder.AddElement<uint64_t>(4U, Timestamp, 0); //  add time under pl72 schema
  builder.AddOffset(12U, startName);  //  add JSON name under pl72 schema
  const flatbuffers::uoffset_t end_ = builder.EndTable(start_);
  auto offsetForFinish = flatbuffers::Offset<RunStart>(end_);
  builder.Finish(offsetForFinish, "mdat");
  flatbuffers::DetachedBuffer msgbuff = builder.Release();
  size_t BufferSize = builder.GetSize();
  auto ReturnBuffer = std::make_unique<uint8_t[]>(BufferSize);
  return {std::move(ReturnBuffer), BufferSize};
}
} // namespace mdat_schema

TEST_F(mdatInit, WriteOneElement) {
  mdat_WriterStandIn TestWriter;
  TestWriter.init_hdf(RootGroup);
  TestWriter.reopen(RootGroup);
  std::int64_t Timestamp{1234}; //  gtest compares int64_t
  auto FlatbufferData =
      mdat_schema::generateFlatbufferMessage(Timestamp);
  FileWriter::FlatbufferMessage FlatbufferMsg(FlatbufferData.first.get(),
                                              FlatbufferData.second);
  EXPECT_EQ(FlatbufferMsg.getFlatbufferID(), "mdat");
  EXPECT_EQ(TestWriter.mdatStart_time.dataspace().size(), 0);
  TestWriter.write(FlatbufferMsg);
  ASSERT_EQ(TestWriter.mdatStart_time.dataspace().size(), 1);
  std::vector<std::int64_t> WrittenTimes(1);
  TestWriter.mdatStart_time.read(WrittenTimes);
  EXPECT_EQ(WrittenTimes.at(0), Timestamp);
}