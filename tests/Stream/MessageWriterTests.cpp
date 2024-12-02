// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "Metrics/Registrar.h"
#include "Stream/MessageWriter.h"
#include "WriterModule/ep01/ep01_Writer.h"
#include "WriterModuleBase.h"
#include "WriterRegistrar.h"
#include "helpers/SetExtractorModule.h"
#include <array>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

class WriterModuleStandIn : public WriterModule::Base {
public:
  MAKE_MOCK0(config_post_processing, void(), override);
  WriterModuleStandIn(std::vector<std::string> const &ExtraModules = {})
      : WriterModule::Base("test", true, "test", ExtraModules) {}
  ~WriterModuleStandIn() = default;
  MAKE_MOCK1(init_hdf, WriterModule::InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(reopen, WriterModule::InitResult(hdf5::node::Group &), override);
  MAKE_MOCK2(writeImpl, void(FileWriter::FlatbufferMessage const &, bool), override);
};

class DataMessageWriterTest : public ::testing::Test {
public:
  void SetUp() override {
    WriterModule::Registry::clear();
    WriterModule::Registry::Registrar<WriterModule::ep01::ep01_Writer>
        RegisterIt1("ep01", "epics_con_info");
  }
  WriterModuleStandIn WriterModule;
};

using trompeloeil::_;

TEST_F(DataMessageWriterTest, NoExtraModules) {
  EXPECT_TRUE(WriterModule.getEnabledExtraModules().empty());
}

TEST_F(DataMessageWriterTest, InvalidExtraModule) {
  WriterModuleStandIn TestWriterModule({"no_such_module"});
  EXPECT_FALSE(TestWriterModule.hasExtraModules());
  EXPECT_TRUE(TestWriterModule.getEnabledExtraModules().empty());
}

TEST_F(DataMessageWriterTest, ValidExtraModule) {
  WriterModuleStandIn TestWriterModule({"ep01"});
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_EQ(TestWriterModule.getEnabledExtraModules().size(), 1u);
}

TEST_F(DataMessageWriterTest, ValidExtraModuleByName) {
  WriterModuleStandIn TestWriterModule({"epics_con_info"});
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_EQ(TestWriterModule.getEnabledExtraModules().size(), 1u);
}

TEST_F(DataMessageWriterTest, DisabledExtraModule) {
  WriterModuleStandIn TestWriterModule({"epics_con_info"});
  REQUIRE_CALL(TestWriterModule, config_post_processing()).TIMES(1);
  TestWriterModule.parse_config(R"({"enable_epics_con_info": false})");
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_TRUE(TestWriterModule.getEnabledExtraModules().empty());
}

TEST_F(DataMessageWriterTest, EnableExtraModule) {
  WriterModuleStandIn TestWriterModule({"epics_con_info"});
  REQUIRE_CALL(TestWriterModule, config_post_processing()).TIMES(1);
  TestWriterModule.parse_config(R"({"enable_epics_con_info": true})");
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_EQ(TestWriterModule.getEnabledExtraModules().size(), 1u);
}

TEST_F(DataMessageWriterTest, WriteMessageSuccess) {
  REQUIRE_CALL(WriterModule, writeImpl(_, _)).TIMES(1);
  auto InitialWriteCount = WriterModule.getWriteCount();
  FileWriter::FlatbufferMessage Msg;
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    Stream::MessageWriter Writer{
        []() {}, 1s, std::make_unique<Metrics::Registrar>("some_prefix")};
    Writer.addMessage(SomeMessage, false);
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWritesDone() == 1);
      EXPECT_TRUE(Writer.nrOfWriteErrors() == 0);
    });
  }
  EXPECT_EQ(InitialWriteCount + 1, WriterModule.getWriteCount());
}

TEST_F(DataMessageWriterTest, WriteMessageExceptionUnknownFb) {
  REQUIRE_CALL(WriterModule, writeImpl(_, _))
      .TIMES(1)
      .THROW(WriterModule::WriterException("Some error."));
  auto InitialWriteCount = WriterModule.getWriteCount();
  FileWriter::FlatbufferMessage Msg;
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    Stream::MessageWriter Writer{
        []() {}, 1s, std::make_unique<Metrics::Registrar>("some_prefix")};
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWriterModulesWithErrors() == 1);
    });
    Writer.addMessage(SomeMessage, false);
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWritesDone() == 0);
      EXPECT_TRUE(Writer.nrOfWriteErrors() == 1);
      EXPECT_TRUE(Writer.nrOfWriterModulesWithErrors() == 1);
    });
  }
  EXPECT_EQ(InitialWriteCount, WriterModule.getWriteCount());
}
