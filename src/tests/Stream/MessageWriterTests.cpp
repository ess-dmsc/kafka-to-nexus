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
#include "WriterModuleBase.h"
#include "helpers/SetExtractorModule.h"
#include <array>
#include <gtest/gtest.h>
#include <trompeloeil.hpp>

class WriterModuleStandIn : public WriterModule::Base {
public:
  MAKE_MOCK1(parse_config, void(std::string const &), override);
  MAKE_MOCK2(init_hdf,
             WriterModule::InitResult(hdf5::node::Group &, std::string const &),
             override);
  MAKE_MOCK1(reopen, WriterModule::InitResult(hdf5::node::Group &), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const &), override);
};

class DataMessageWriterStandIn : public Stream::MessageWriter {
public:
  DataMessageWriterStandIn(Metrics::Registrar const &Registrar)
      : MessageWriter(Registrar) {}
  using MessageWriter::Executor;
  using MessageWriter::ModuleErrorCounters;
  using MessageWriter::WriteErrors;
  using MessageWriter::WritesDone;
};

class DataMessageWriterTest : public ::testing::Test {
public:
  WriterModuleStandIn WriterModule;
  Metrics::Registrar MetReg{"some_prefix", {}};
};

using trompeloeil::_;

TEST_F(DataMessageWriterTest, WriteMessageSuccess) {
  REQUIRE_CALL(WriterModule, write(_)).TIMES(1);
  FileWriter::FlatbufferMessage Msg;
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    DataMessageWriterStandIn Writer{MetReg};
    Writer.addMessage(SomeMessage);
    Writer.Executor.SendWork([&Writer]() {
      EXPECT_TRUE(Writer.WritesDone == 1);
      EXPECT_TRUE(Writer.WriteErrors == 0);
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
    DataMessageWriterStandIn Writer{MetReg};
    Writer.Executor.SendWork(
        [&Writer]() { EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 1); });
    Writer.addMessage(SomeMessage);
    Writer.Executor.SendWork([&Writer]() {
      EXPECT_TRUE(Writer.WritesDone == 0);
      EXPECT_TRUE(Writer.WriteErrors == 1);
      EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 1);
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
  std::array<char, 9> SomeData{'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'};
  setExtractorModule<xxxFbReader>("xxxx");
  FileWriter::FlatbufferMessage Msg(SomeData.data(), SomeData.size());
  Stream::Message SomeMessage(
      reinterpret_cast<Stream::Message::DestPtrType>(&WriterModule), Msg);
  {
    DataMessageWriterStandIn Writer{MetReg};
    Writer.Executor.SendWork(
        [&Writer]() { EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 1); });
    Writer.addMessage(SomeMessage);
    Writer.Executor.SendWork([&Writer]() {
      EXPECT_TRUE(Writer.WritesDone == 0);
      EXPECT_TRUE(Writer.WriteErrors == 1);
      EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 2);
    });
  }
}
