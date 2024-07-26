// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include "FlatbufferReader.h"
#include "Stream/SourceFilter.h"
#include "WriterModule/template/TemplateWriter.h"
#include "helpers/SetExtractorModule.h"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>
#include <trompeloeil.hpp>

class MessageWriterStandIn : public Stream::MessageWriter {
public:
  MessageWriterStandIn()
      : MessageWriter([]() {}, 1s, std::make_unique<Metrics::Registrar>("")) {}
  MAKE_MOCK1(addMessage, void(Stream::Message const &), override);
};

class SourceFilterStandIn : public Stream::SourceFilter {
public:
  SourceFilterStandIn(time_point Start, time_point Stop,
                      bool AcceptRepeatedTimestamp,
                      Stream::MessageWriter *Writer,
                      std::unique_ptr<Metrics::IRegistrar> Reg)
      : SourceFilter(Start, Stop, AcceptRepeatedTimestamp, Writer,
                     std::move(Reg)) {}
  using SourceFilter::MessagesDiscarded;
  using SourceFilter::MessagesReceived;
  using SourceFilter::MessagesTransmitted;
  using SourceFilter::RepeatedTimestamp;
  using SourceFilter::UnorderedTimestamp;
};

class SourceFilterTest : public ::testing::Test {
public:
  time_point StartTime{std::chrono::system_clock::now()};
  MessageWriterStandIn Writer;
  auto getTestFilter(bool AcceptRepeatedTimestamp = true) {
    return std::make_unique<SourceFilterStandIn>(
        StartTime, std::chrono::system_clock::time_point::max(),
        AcceptRepeatedTimestamp, &Writer,
        std::make_unique<Metrics::Registrar>("test_reg"));
  }
};

TEST_F(SourceFilterTest, InitState) {
  auto UnderTest = getTestFilter();
  EXPECT_FALSE(UnderTest->hasFinished());
}
using trompeloeil::_;

class yyyyFbReader : public FileWriter::FlatbufferReader {
public:
  bool verify(FileWriter::FlatbufferMessage const &) const override {
    return true;
  }

  std::string
  source_name(FileWriter::FlatbufferMessage const &) const override {
    return "some_name";
  }

  uint64_t timestamp(FileWriter::FlatbufferMessage const &) const override {
    return yyyyFbReader::Timestamp;
  }
  static void setTimestamp(uint64_t NewTime) {
    yyyyFbReader::Timestamp = NewTime;
  }

private:
  static uint64_t Timestamp;
};

uint64_t yyyyFbReader::Timestamp{1};

FileWriter::FlatbufferMessage generateMsg() {
  std::array<uint8_t, 9> SomeData{'y', 'y', 'y', 'y', 'y', 'y', 'y', 'y', 'y'};
  setExtractorModule<yyyyFbReader>("yyyy");
  return FileWriter::FlatbufferMessage(SomeData.data(), SomeData.size());
}

TEST_F(SourceFilterTest, InvalidMessage) {
  FORBID_CALL(Writer, addMessage(_));
  auto UnderTest = getTestFilter();
  FileWriter::FlatbufferMessage TestMsg;
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesDiscarded == 1);
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, MessageWithNoDest) {
  FORBID_CALL(Writer, addMessage(_));
  auto UnderTest = getTestFilter();
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
}

TEST_F(SourceFilterTest, SendBufferedMessageFromBeforeStart) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(1);
  {
    auto UnderTest = getTestFilter();
    UnderTest->add_writer_module_for_message(0);
    auto TestMsg = generateMsg();
    UnderTest->filterMessage(std::move(TestMsg));
    EXPECT_TRUE(UnderTest->MessagesReceived == 1);
    EXPECT_TRUE(UnderTest->MessagesTransmitted == 0);
    EXPECT_FALSE(UnderTest->hasFinished());
  } // Have destructor send message
}

TEST_F(SourceFilterTest, MultipleMessagesBeforeStart) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(1);
  auto UnderTest = getTestFilter();
  UnderTest->add_writer_module_for_message(0);
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  yyyyFbReader::setTimestamp(2);
  auto TestMsg2 = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg2));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 0);
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, SameTSBeforeStart) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(1);
  auto UnderTest = getTestFilter();
  UnderTest->add_writer_module_for_message(0);
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->RepeatedTimestamp == 1);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 0);
  EXPECT_FALSE(UnderTest->hasFinished());
}

using std::chrono_literals::operator""ms;

TEST_F(SourceFilterTest, SameTSAfterStartAccepted) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(2);
  auto UnderTest = getTestFilter();
  UnderTest->add_writer_module_for_message(0);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->RepeatedTimestamp == 1);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 2);
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, SameTSAfterStartRejected) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(1);
  auto UnderTest = getTestFilter(false);
  UnderTest->add_writer_module_for_message(0);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->RepeatedTimestamp == 1);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 1);
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, MsgBeforeAndAfterStart) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(2);
  auto UnderTest = getTestFilter();
  UnderTest->add_writer_module_for_message(0);
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
  auto TestMsg2 = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg2));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 2);
  EXPECT_FALSE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, MultipleDestinations) {
  TemplateWriter::WriterClass Writer1;
  TemplateWriter::WriterClass Writer2;
  {
    REQUIRE_CALL(Writer, addMessage(_))
        .LR_WITH(_1.DestPtr == &Writer1)
        .TIMES(1);
    REQUIRE_CALL(Writer, addMessage(_))
        .LR_WITH(_1.DestPtr == &Writer2)
        .TIMES(1);
    auto UnderTest = getTestFilter();
    UnderTest->add_writer_module_for_message(&Writer1);
    UnderTest->add_writer_module_for_message(&Writer2);
    yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
    auto TestMsg = generateMsg();
    UnderTest->filterMessage(std::move(TestMsg));
    EXPECT_TRUE(UnderTest->MessagesReceived == 1);
    EXPECT_TRUE(UnderTest->MessagesTransmitted == 1);
    EXPECT_FALSE(UnderTest->hasFinished());
  }
}

TEST_F(SourceFilterTest, MessageAfterStop) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(1);
  auto UnderTest = getTestFilter();
  TemplateWriter::WriterClass Writer1;
  UnderTest->add_writer_module_for_message(&Writer1);
  UnderTest->setStopTime(StartTime + 20ms);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesReceived == 1);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 1);
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, MessageBeforeAndAfterStop) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(2);
  auto UnderTest = getTestFilter();
  TemplateWriter::WriterClass Writer1;
  UnderTest->add_writer_module_for_message(&Writer1);
  UnderTest->setStopTime(StartTime + 20ms);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 10ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));

  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
  auto TestMsg2 = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg2));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 2);
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, MessageBeforeStartAndAfterStop) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(2);
  auto UnderTest = getTestFilter();
  TemplateWriter::WriterClass Writer1;
  UnderTest->add_writer_module_for_message(&Writer1);
  UnderTest->setStopTime(StartTime + 20ms);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime - 10ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 0);

  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 50ms));
  auto TestMsg2 = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg2));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 2);
  EXPECT_TRUE(UnderTest->hasFinished());
}

TEST_F(SourceFilterTest, UnorderedMsgBeforeStart) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(1);
  auto UnderTest = getTestFilter();
  TemplateWriter::WriterClass Writer1;
  UnderTest->add_writer_module_for_message(&Writer1);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime - 10ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 0);

  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime - 50ms));
  auto TestMsg2 = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg2));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->UnorderedTimestamp == 1);
  EXPECT_TRUE(UnderTest->MessagesDiscarded == 1);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 0);
}

TEST_F(SourceFilterTest, UnorderedMsgAfterStart) {
  REQUIRE_CALL(Writer, addMessage(_)).TIMES(2);
  auto UnderTest = getTestFilter();
  TemplateWriter::WriterClass Writer1;
  UnderTest->add_writer_module_for_message(&Writer1);
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 10ms));
  auto TestMsg = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg));
  yyyyFbReader::setTimestamp(toNanoSeconds(StartTime + 5ms));
  auto TestMsg2 = generateMsg();
  UnderTest->filterMessage(std::move(TestMsg2));
  EXPECT_TRUE(UnderTest->MessagesReceived == 2);
  EXPECT_TRUE(UnderTest->UnorderedTimestamp == 1);
  EXPECT_TRUE(UnderTest->MessagesDiscarded == 0);
  EXPECT_TRUE(UnderTest->MessagesTransmitted == 2);
}
