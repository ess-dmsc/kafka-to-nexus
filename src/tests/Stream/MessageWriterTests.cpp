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
#include "WriterModule/ep00/ep00_Writer.h"
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
      : WriterModule::Base(true, "test", ExtraModules) {}
  ~WriterModuleStandIn() = default;
  MAKE_CONST_MOCK1(init_hdf, WriterModule::InitResult(hdf5::node::Group &),
                   override);
  MAKE_MOCK1(reopen, WriterModule::InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const &), override);
};

class DataMessageWriterTest : public ::testing::Test {
public:
  void SetUp() override {
    WriterModule::Registry::clear();
    WriterModule::Registry::Registrar<WriterModule::ep00::ep00_Writer>
        RegisterIt1("ep00", "epics_con_status");
  }
  WriterModuleStandIn WriterModule;
  Metrics::Registrar MetReg{"some_prefix", {}};
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
  WriterModuleStandIn TestWriterModule({"ep00"});
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_EQ(TestWriterModule.getEnabledExtraModules().size(), 1u);
}

TEST_F(DataMessageWriterTest, ValidExtraModuleByName) {
  WriterModuleStandIn TestWriterModule({"epics_con_status"});
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_EQ(TestWriterModule.getEnabledExtraModules().size(), 1u);
}

TEST_F(DataMessageWriterTest, DisabledExtraModule) {
  WriterModuleStandIn TestWriterModule({"epics_con_status"});
  REQUIRE_CALL(TestWriterModule, config_post_processing()).TIMES(1);
  TestWriterModule.parse_config("{\"enable_epics_con_status\": false}");
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_TRUE(TestWriterModule.getEnabledExtraModules().empty());
}

TEST_F(DataMessageWriterTest, EnableExtraModule) {
  WriterModuleStandIn TestWriterModule({"epics_con_status"});
  REQUIRE_CALL(TestWriterModule, config_post_processing()).TIMES(1);
  TestWriterModule.parse_config("{\"enable_epics_con_status\": true}");
  EXPECT_TRUE(TestWriterModule.hasExtraModules());
  EXPECT_EQ(TestWriterModule.getEnabledExtraModules().size(), 1u);
}

TEST_F(DataMessageWriterTest, WriteMessageSuccess) {
  REQUIRE_CALL(WriterModule, write(_)).TIMES(1);
  FileWriter::FlatbufferMessage Msg;
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    Stream::MessageWriter Writer{[]() {}, 1s, MetReg};
    Writer.addMessage(SomeMessage);
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWritesDone() == 1);
      EXPECT_TRUE(Writer.nrOfWriteErrors() == 0);
    });
  }
}

TEST_F(DataMessageWriterTest, WriteMessageExceptionUnknownFb) {
  REQUIRE_CALL(WriterModule, write(_))
      .TIMES(1)
      .THROW(WriterModule::WriterException("Some error."));
  FileWriter::FlatbufferMessage Msg;
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    Stream::MessageWriter Writer{[]() {}, 1s, MetReg};
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWriterModulesWithErrors() == 1);
    });
    Writer.addMessage(SomeMessage);
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWritesDone() == 0);
      EXPECT_TRUE(Writer.nrOfWriteErrors() == 1);
      EXPECT_TRUE(Writer.nrOfWriterModulesWithErrors() == 1);
    });
  }
}

class xxxFbReader : public FileWriter::FlatbufferReader {
  bool verify(FileWriter::FlatbufferMessage const &) const override {
    return true;
  }

  std::string
  source_name(FileWriter::FlatbufferMessage const &) const override {
    return "some_name";
  }

  uint64_t timestamp(FileWriter::FlatbufferMessage const &) const override {
    return 1;
  }
};

TEST_F(DataMessageWriterTest, WriteMessageExceptionKnownFb) {
  REQUIRE_CALL(WriterModule, write(_))
      .TIMES(1)
      .THROW(WriterModule::WriterException("Some error."));
  std::array<uint8_t, 9> SomeData{'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'};
  setExtractorModule<xxxFbReader>("xxxx");
  FileWriter::FlatbufferMessage Msg(SomeData.data(), SomeData.size());
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    Stream::MessageWriter Writer{[]() {}, 1s, MetReg};
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWriterModulesWithErrors() == 1);
    });
    Writer.addMessage(SomeMessage);
    Writer.runJob([&Writer]() {
      EXPECT_TRUE(Writer.nrOfWritesDone() == 0);
      EXPECT_TRUE(Writer.nrOfWriteErrors() == 1);
      EXPECT_TRUE(Writer.nrOfWriterModulesWithErrors() == 2);
    });
  }
}
