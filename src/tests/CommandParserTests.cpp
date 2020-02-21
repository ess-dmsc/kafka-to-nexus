// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#include <6s4t_run_stop_generated.h>
#include <chrono>
#include <gtest/gtest.h>
#include <pl72_run_start_generated.h>

#include "CommandParser.h"
#include "Msg.h"
#include "helpers/RunStartStopHelpers.h"

using namespace RunStartStopHelpers;

std::string const InstrumentNameInput = "TEST";
std::string const RunNameInput = "42";
std::string const NexusStructureInput = "{}";
std::string const JobIDInput = "qw3rty";
std::string const ServiceIDInput = "filewriter1";
std::string const BrokerInput = "somehost:1234";
std::string const FilenameInput = "a-dummy-name-01.h5";
uint64_t const StartTimeInput = 123456789000;
uint64_t const StopTimeInput = 123456790000;

class CommandParserHappyStartTests : public testing::Test {
public:
  FileWriter::StartCommandInfo StartInfo;

  // cppcheck-suppress unusedFunction
  void SetUp() override {
    auto MessageBuffer = buildRunStartMessage(
        InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
        ServiceIDInput, BrokerInput, FilenameInput, StartTimeInput,
        StopTimeInput);

    StartInfo =
        FileWriter::CommandParser::extractStartInformation(MessageBuffer);
  }
};

TEST_F(CommandParserHappyStartTests, IfJobIDPresentThenExtractedCorrectly) {
  ASSERT_EQ(JobIDInput, StartInfo.JobID);
}

TEST_F(CommandParserHappyStartTests, IfFilenamePresentThenExtractedCorrectly) {
  ASSERT_EQ(FilenameInput, StartInfo.Filename);
}

TEST_F(CommandParserHappyStartTests, IfBrokerPresentThenExtractedCorrectly) {
  ASSERT_EQ(BrokerInput, StartInfo.BrokerInfo.HostPort);
  ASSERT_EQ(1234u, StartInfo.BrokerInfo.Port);
}

TEST_F(CommandParserHappyStartTests,
       IfNexusStructurePresentThenExtractedCorrectly) {
  ASSERT_EQ(NexusStructureInput, StartInfo.NexusStructure);
}

TEST_F(CommandParserHappyStartTests, IfStartPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{StartTimeInput}, StartInfo.StartTime);
}

TEST_F(CommandParserHappyStartTests, IfStopPresentThenExtractedCorrectly) {
  ASSERT_EQ(std::chrono::milliseconds{StopTimeInput}, StartInfo.StopTime);
}

TEST_F(CommandParserHappyStartTests, IfServiceIdPresentThenExtractedCorrectly) {
  ASSERT_EQ(ServiceIDInput, StartInfo.ServiceID);
}

TEST(CommandParserSadStartTests, ThrowsIfNoJobID) {
  std::string const EmptyJobID;
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, EmptyJobID,
      ServiceIDInput, BrokerInput, FilenameInput, StartTimeInput,
      StopTimeInput);

  ASSERT_THROW(
      FileWriter::CommandParser::extractStartInformation(MessageBuffer),
      std::runtime_error);
}

TEST(CommandParserSadStartTests, ThrowsIfNoFilename) {
  std::string const EmptyFilename;
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
      ServiceIDInput, BrokerInput, EmptyFilename, StartTimeInput,
      StopTimeInput);

  ASSERT_THROW(
      FileWriter::CommandParser::extractStartInformation(MessageBuffer),
      std::runtime_error);
}

TEST(CommandParserSadStartTests, ThrowsIfNoNexusStructure) {
  std::string const EmptyNexusStructure;
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, EmptyNexusStructure, JobIDInput,
      ServiceIDInput, BrokerInput, FilenameInput, StartTimeInput,
      StopTimeInput);

  ASSERT_THROW(
      FileWriter::CommandParser::extractStartInformation(MessageBuffer),
      std::runtime_error);
}

TEST(CommandParserSadStartTests, IfNoBrokerThenThrows) {
  std::string const EmptyBroker;
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
      ServiceIDInput, EmptyBroker, FilenameInput, StartTimeInput,
      StopTimeInput);

  ASSERT_THROW(
      FileWriter::CommandParser::extractStartInformation(MessageBuffer),
      std::runtime_error);
}

TEST(CommandParserSadStartTests, IfBrokerIsWrongFormThenThrows) {
  std::string const BrokerInvalidFormat = "1234:somehost";
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
      ServiceIDInput, BrokerInvalidFormat, FilenameInput, StartTimeInput,
      StopTimeInput);

  ASSERT_THROW(
      FileWriter::CommandParser::extractStartInformation(MessageBuffer),
      std::runtime_error);
}

TEST(CommandParserStartTests, IfNoStartTimeThenUsesSuppliedCurrentTime) {
  // Start time from flatbuffer is 0 if not supplied when message constructed
  uint64_t const NoStartTime = 0;
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
      ServiceIDInput, BrokerInput, FilenameInput, NoStartTime, StopTimeInput);

  auto FakeCurrentTime = std::chrono::milliseconds{987654321};

  auto StartInfo = FileWriter::CommandParser::extractStartInformation(
      MessageBuffer, FakeCurrentTime);

  ASSERT_EQ(FakeCurrentTime, StartInfo.StartTime);
}

TEST(CommandParserStartTests, IfNoServiceIdThenIsBlank) {
  std::string const EmptyServiceID;
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
      EmptyServiceID, BrokerInput, FilenameInput, StartTimeInput,
      StopTimeInput);

  auto StartInfo =
      FileWriter::CommandParser::extractStartInformation(MessageBuffer);

  ASSERT_EQ("", StartInfo.ServiceID);
}

TEST(CommandParserSadStopTests, IfNoJobIdThenThrows) {
  std::string const EmptyJobID;
  auto MessageBuffer = buildRunStopMessage(StopTimeInput, RunNameInput,
                                           EmptyJobID, ServiceIDInput);

  ASSERT_THROW(FileWriter::CommandParser::extractStopInformation(MessageBuffer),
               std::runtime_error);
}

TEST(CommandParserHappyStopTests, IfJobIdPresentThenExtractedCorrectly) {
  auto MessageBuffer = buildRunStopMessage(StopTimeInput, RunNameInput,
                                           JobIDInput, ServiceIDInput);

  auto StopInfo =
      FileWriter::CommandParser::extractStopInformation(MessageBuffer);

  ASSERT_EQ(JobIDInput, StopInfo.JobID);
}

TEST(CommandParserHappyStopTests, IfStopTimePresentThenExtractedCorrectly) {
  auto MessageBuffer = buildRunStopMessage(StopTimeInput, RunNameInput,
                                           JobIDInput, ServiceIDInput);

  auto StopInfo =
      FileWriter::CommandParser::extractStopInformation(MessageBuffer);

  ASSERT_EQ(std::chrono::milliseconds{StopTimeInput}, StopInfo.StopTime);
}

TEST(CommandParserStopTests, IfNoServiceIdThenIsBlank) {
  std::string const EmptyServiceID;
  auto MessageBuffer = buildRunStopMessage(StopTimeInput, RunNameInput,
                                           JobIDInput, EmptyServiceID);

  auto StopInfo =
      FileWriter::CommandParser::extractStopInformation(MessageBuffer);

  ASSERT_EQ("", StopInfo.ServiceID);
}

TEST(CommandParserStopTests, IfServiceIdPresentThenExtractedCorrectly) {
  auto MessageBuffer = buildRunStopMessage(StopTimeInput, RunNameInput,
                                           JobIDInput, ServiceIDInput);

  auto StopInfo =
      FileWriter::CommandParser::extractStopInformation(MessageBuffer);

  ASSERT_EQ(ServiceIDInput, StopInfo.ServiceID);
}

TEST(CommandParserStartTests,
     MessageIsNotStartCommandIfFlatbufferIDNotRecognised) {
  std::string const MessageString = "00001234";
  FileWriter::Msg const TestMessage{MessageString.c_str(),
                                    MessageString.size()};
  ASSERT_FALSE(FileWriter::CommandParser::isStartCommand(TestMessage));
}

TEST(CommandParserStartTests,
     MessageIsNotStartCommandIfFlatbufferFailsVerification) {
  std::string const MessageString = fmt::format("0000{}", RunStartIdentifier());
  FileWriter::Msg const TestMessage{MessageString.c_str(),
                                    MessageString.size()};
  ASSERT_FALSE(FileWriter::CommandParser::isStartCommand(TestMessage));
}

TEST(CommandParserStartTests, MessageIsStartCommandIfValidRunStartFlatbuffer) {
  auto MessageBuffer = buildRunStartMessage(
      InstrumentNameInput, RunNameInput, NexusStructureInput, JobIDInput,
      ServiceIDInput, BrokerInput, FilenameInput, StartTimeInput,
      StopTimeInput);
  FileWriter::Msg const TestMessage{MessageBuffer.data(), MessageBuffer.size()};
  ASSERT_TRUE(FileWriter::CommandParser::isStartCommand(TestMessage));
}

TEST(CommandParserStopTests,
     MessageIsNotStopCommandIfFlatbufferIDNotRecognised) {
  std::string const MessageString = "00001234";
  FileWriter::Msg const TestMessage{MessageString.c_str(),
                                    MessageString.size()};
  ASSERT_FALSE(FileWriter::CommandParser::isStopCommand(TestMessage));
}

TEST(CommandParserStopTests,
     MessageIsNotStopCommandIfFlatbufferFailsVerification) {
  std::string const MessageString = fmt::format("0000{}", RunStopIdentifier());
  FileWriter::Msg const TestMessage{MessageString.c_str(),
                                    MessageString.size()};
  ASSERT_FALSE(FileWriter::CommandParser::isStopCommand(TestMessage));
}

TEST(CommandParserStopTests, MessageIsStartCommandIfValidRunStopFlatbuffer) {
  auto MessageBuffer = buildRunStopMessage(StopTimeInput, RunNameInput,
                                           JobIDInput, ServiceIDInput);
  FileWriter::Msg const TestMessage{MessageBuffer.data(), MessageBuffer.size()};
  ASSERT_TRUE(FileWriter::CommandParser::isStopCommand(TestMessage));
}
