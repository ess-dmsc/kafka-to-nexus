// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "DataMessageWriter.h"
#include "WriterModuleBase.h"
#include <gtest/gtest.h>
#include <trompeloeil.hpp>
#include "Metrics/Registrar.h"
#include <array>

class WriterModuleStandIn : public WriterModule::Base {
public:
  MAKE_MOCK1(parse_config, void(std::string const&), override);
  MAKE_MOCK2(init_hdf, WriterModule::InitResult(hdf5::node::Group&, std::string const&), override);
  MAKE_MOCK1(reopen, WriterModule::InitResult(hdf5::node::Group&), override);
  MAKE_MOCK1(write, void(FileWriter::FlatbufferMessage const&), override);
};

class DataMessageWriterStandIn : public DataMessageWriter {
public:
  DataMessageWriterStandIn(Metrics::Registrar const& Registrar) : DataMessageWriter(Registrar){}
  using DataMessageWriter::WritesDone;
  using DataMessageWriter::WriteErrors;
  using DataMessageWriter::Executor;
  using DataMessageWriter::ModuleErrorCounters;
};

class DataMessageWriterTest : public ::testing::Test {
public:
  WriterModuleStandIn WriterModule;
  Metrics::Registrar MetReg{"some_prefix", {}};
};

using trompeloeil::_;

TEST_F(DataMessageWriterTest, WriteMessageSuccess) {
  REQUIRE_CALL(WriterModule, write(_)).TIMES(1);
  WriteMessage SomeMessage(reinterpret_cast<WriteMessage::DstId>(&WriterModule), Msg);
  {
    DataMessageWriterStandIn Writer{MetReg};
    Writer.addMessage(SomeMessage);
    Writer.Executor.SendWork([&Writer](){
      EXPECT_TRUE(Writer.WritesDone == 1);
      EXPECT_TRUE(Writer.WriteErrors == 0);
    });
  }
}

TEST_F(DataMessageWriterTest, WriteMessageExceptionUnknownFb) {
  REQUIRE_CALL(WriterModule, write(_)).TIMES(1).THROW(WriterModule::WriterException("Some error."));
  FileWriter::FlatbufferMessage Msg;
  WriteMessage SomeMessage(reinterpret_cast<WriteMessage::DstId>(&WriterModule), Msg);
  {
    DataMessageWriterStandIn Writer{MetReg};
    Writer.Executor.SendWork([&Writer](){
      EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 1);
    });
    Writer.addMessage(SomeMessage);
    Writer.Executor.SendWork([&Writer](){
      EXPECT_TRUE(Writer.WritesDone == 0);
      EXPECT_TRUE(Writer.WriteErrors == 1);
      EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 1);
    });
  }
}

TEST_F(DataMessageWriterTest, WriteMessageExceptionKnownFb) {
  REQUIRE_CALL(WriterModule, write(_)).TIMES(1).THROW(WriterModule::WriterException("Some error."));
  std::array<char,8> SomeData{'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'};
  FileWriter::FlatbufferMessage Msg(SomeData.data(), SomeData.size());
  WriteMessage SomeMessage(reinterpret_cast<WriteMessage::DstId>(&WriterModule), Msg);
  {
    DataMessageWriterStandIn Writer{MetReg};
    Writer.Executor.SendWork([&Writer](){
      EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 1);
    });
    Writer.addMessage(SomeMessage);
    Writer.Executor.SendWork([&Writer](){
      EXPECT_TRUE(Writer.WritesDone == 0);
      EXPECT_TRUE(Writer.WriteErrors == 1);
      EXPECT_TRUE(Writer.ModuleErrorCounters.size() == 2);
    });
  }
}
